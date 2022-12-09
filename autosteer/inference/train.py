"""This module trains and evaluates the Bao integration for Presto"""
import os
import storage
from autosteer.inference import model
import numpy as np
import pickle
from performance_prediction import PerformancePrediction
from autosteer.inference.net import DROPOUT
from custom_logging import autosteer_logging


class AutoSteerInferenceException(Exception):
    pass


def load_data(bench=None, training_ratio=0.8):
    training_data, test_data = storage.experience(bench, training_ratio)

    x_train = [config.plan_json for config in training_data]
    y_train = [config.running_time for config in training_data]
    x_test = [config.plan_json for config in test_data]
    y_test = [config.running_time for config in test_data]

    return x_train, y_train, x_test, y_test, training_data, test_data


def serialize_data(directory, x_train, y_train, x_test, y_test, training_configs, test_configs):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(f'{directory}/x_train', 'wb') as f:
        pickle.dump(x_train, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/y_train', 'wb') as f:
        pickle.dump(y_train, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/x_test', 'wb') as f:
        pickle.dump(x_test, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/y_test', 'wb') as f:
        pickle.dump(y_test, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/training_configs', 'wb') as f:
        pickle.dump(training_configs, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/test_configs', 'wb') as f:
        pickle.dump(test_configs, f, pickle.HIGHEST_PROTOCOL)


def deserialize_data(directory):
    with open(f'{directory}/x_train', 'rb') as f:
        x_train = pickle.load(f)
    with open(f'{directory}/y_train', 'rb') as f:
        y_train = pickle.load(f)
    with open(f'{directory}/x_test', 'rb') as f:
        x_test = pickle.load(f)
    with open(f'{directory}/y_test', 'rb') as f:
        y_test = pickle.load(f)
    with open(f'{directory}/training_configs', 'rb') as f:
        training_configs = pickle.load(f)
    with open(f'{directory}/test_configs', 'rb') as f:
        test_configs = pickle.load(f)
    return x_train, y_train, x_test, y_test, training_configs, test_configs


def train_and_save_model(filename, x_train, y_train, x_test, y_test, verbose=True):
    autosteer_logging.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if not x_train:
        raise AutoSteerInferenceException('Cannot train a Bao model with no experience')

    if len(x_train) < 20:
        autosteer_logging.warning('Warning: trying to train a Bao model with fewer than 20 datapoints.')

    regression_model = model.BaoRegressionModel(verbose=verbose)
    losses = regression_model.fit(x_train, y_train, x_test, y_test)
    regression_model.save(filename)

    return regression_model, losses


def evaluate_prediction(y, predictions, plans, query_path, is_training) -> PerformancePrediction:
    default_plan = list(filter(lambda x: x.num_disabled_rules == 0, plans))[0]

    autosteer_logging.info('y:\t%s', '\t'.join([f'{_:.2f}' for _ in y]))
    autosteer_logging.info('ŷ:\t%s', '\t'.join(f'{prediction[0]:.2f}' for prediction in predictions))
    # the plan index which is estimated to perform best by Bao
    min_prediction_index = np.argmin(predictions)
    autosteer_logging.info('min predicted index: %s (smaller is better)', str(min_prediction_index))

    # evaluate performance gains with Bao
    performance_from_model = y[min_prediction_index]
    autosteer_logging.info('best choice -> %s', str(y[0] / default_plan.running_time))

    if performance_from_model < default_plan.running_time:
        autosteer_logging.info('good choice -> %s', str(performance_from_model / default_plan.running_time))
    else:
        autosteer_logging.info('bad choice -> %s', str(performance_from_model / default_plan.running_time))

    # best alternative configuration is either the first or second one
    best_alt_plan_running_time = plans[0].running_time if plans[0].num_disabled_rules > 0 else plans[1].running_time
    return PerformancePrediction(default_plan.running_time, plans[min_prediction_index].running_time, best_alt_plan_running_time, query_path, is_training)


def choose_best_plans(filename: str, test_configs: list[storage.Measurement], is_training: bool) -> list[PerformancePrediction]:
    """For each query, let Bao estimate the performance of all QEPs and compare them to the runtime of the default plan"""

    # load model
    bao_model = model.BaoRegressionModel(verbose=True)
    bao_model.load(filename)

    # load query plans for prediction
    all_query_plans = {}
    for plan_runtime in test_configs:
        if plan_runtime.query_id in all_query_plans:
            all_query_plans[plan_runtime.query_id].append(plan_runtime)
        else:
            all_query_plans[plan_runtime.query_id] = [plan_runtime]

    performance_predictions: list[PerformancePrediction] = []

    for query_id in sorted(all_query_plans.keys()):
        plans_and_estimates = all_query_plans[query_id]
        plans_and_estimates = sorted(plans_and_estimates, key=lambda record: record.running_time)
        query_path = plans_and_estimates[0].query_path

        autosteer_logging.info('Preprocess data for query %s', plans_and_estimates[0].query_path)
        x = [x.plan_json for x in plans_and_estimates]
        y = [x.running_time for x in plans_and_estimates]

        predictions = bao_model.predict(x)
        performance_prediction = evaluate_prediction(y, predictions, plans_and_estimates, query_path, is_training)
        performance_predictions.append(performance_prediction)
    return list(reversed(sorted(performance_predictions, key=lambda entry: entry.selected_plan_relative_improvement)))


def train(bench: str, considered_queries_in_plot: list[str]):
    model_name = 'postgres_model'
    data_path = 'postgres_data'
    retrain = False
    create_datasets = False

    if create_datasets:
        x_train, y_train, x_test, y_test, training_data, test_data = load_data(bench, training_ratio=0.85)
        serialize_data(data_path, x_train, y_train, x_test, y_test, training_data, test_data)
    else:
        x_train, y_train, x_test, y_test, training_data, test_data = deserialize_data(data_path)
        autosteer_logging.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if retrain:
        _, (training_loss, test_loss) = train_and_save_model(model_name, x_train, y_train, x_test, y_test)
        plt.plot(range(len(training_loss)), training_loss, label='training')
        plt.plot(range(len(test_loss)), test_loss, label='test')
        plt.savefig(f'evaluation/training/losses_1dropout_{DROPOUT}.pdf')

    else:
        x_train, y_train, x_test, y_test, training_data, test_data = deserialize_data(data_path)

    # todo evaluate also the improvements of the best hint sets here!!!
    performance_test = choose_best_plans(model_name, test_data, is_training=False)
    performance_training = choose_best_plans(model_name, training_data, is_training=True)

    # calculate absolute improvements for test and training sets
    def calc_improvements(title: str, dataset: list):
        abs_runtime_bao = float(sum([x.selected_plan_runtime for x in dataset]))
        abs_runtime_best_hs = float(sum([x.best_alt_plan_runtime for x in dataset]))
        abs_runtime_test_default_plan = float(sum([x.default_plan_runtime for x in dataset]))

        results = f'----------------------------------------\n' \
                  f'{title}\n' \
                  f'----------------------------------------\n' \
                  f'overall runtime of default plans: {abs_runtime_test_default_plan}\n' \
                  f'overall runtime of bao selected plans: {abs_runtime_bao}\n' \
                  f'overall runtime of best hs plans: {abs_runtime_best_hs}\n' \
                  f'test improvement rel w/ bao: {1.0 - (abs_runtime_bao / float(abs_runtime_test_default_plan)):.4f}\n' \
                  f'test improvement abs w/ bao: {abs_runtime_bao - abs_runtime_test_default_plan}\n' \
                  f'test improvement rel of best alternative hs: {1.0 - (abs_runtime_best_hs / float(abs_runtime_test_default_plan)):.4f}\n' \
                  f'test improvement abs of best alternative hs: {abs_runtime_best_hs - abs_runtime_test_default_plan}\n'
        return results

    with open(f'evaluation/training/results_{DROPOUT}.csv', 'a', encoding='utf-8') as f:
        f.write(calc_improvements('TEST SET', performance_test))
        f.write(calc_improvements('TRAINING SET', performance_training))

    # sample down before plotting
    performance_test = list(filter(lambda performance_pred: performance_pred.query_path in considered_queries_in_plot, performance_test))
    performance_training = list(filter(lambda performance_pred: performance_pred.query_path in considered_queries_in_plot, performance_training))


if __name__ == '__main__':
    for benchmark in ['job']:
        best_alternative_configs = storage.best_alternative_configuration(benchmark)

        all_indices = list(range(len(best_alternative_configs)))

        # sample a uniform subset of queries for readability
        #indicies = random.sample(range(len(best_alternative_configs)), int(len(best_alternative_configs) * 0.6))
        #indicies = np.array(indicies)
        #np.save('presto_evaluation_indices', indicies)
        indicies = np.load('presto_evaluation_indices.npy')

        # 1st: Plot the best hint sets
        #todo uncomment plot_performance(benchmark, best_alternative_configs, indicies, 'relative', width=8.5, height=2.05)
        ### plot_performance(benchmark, best_alternative_configs, indicies, 'absolute')

        # 2nd: Plot Bao predicted best hint sets and compare to the actual best hint sets
        queries_for_plotting = [best_alternative_configs[i].path for i in indicies]
        train(benchmark, queries_for_plotting)
