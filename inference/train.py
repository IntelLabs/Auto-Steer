# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module trains and evaluates tree convolutional neural networks based on AutoSteer's discovered and executed query plans"""
import os
import storage
import numpy as np
import pickle
from matplotlib import pyplot as plt

from inference.performance_prediction import PerformancePrediction
from inference import model
from inference.net import DROPOUT
from utils.custom_logging import logger


class AutoSteerInferenceException(Exception):
    """Exceptions raised in the inference mode"""
    pass


def _load_data(bench=None, training_ratio=0.8):
    """Load the training and test data for a specific benchmark"""
    training_data, test_data = storage.experience(bench, training_ratio)

    x_train = [config.plan_json for config in training_data]
    y_train = [config.walltime for config in training_data]
    x_test = [config.plan_json for config in test_data]
    y_test = [config.walltime for config in test_data]

    return x_train, y_train, x_test, y_test, training_data, test_data


def _serialize_data(directory, x_train, y_train, x_test, y_test, training_configs, test_configs):
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


def _deserialize_data(directory):
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


def _train_and_save_model(preprocessor, filename, x_train, y_train, x_test, y_test):
    logger.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if not x_train:
        raise AutoSteerInferenceException('Cannot train a TCNN model with no experience')

    if len(x_train) < 20:
        logger.warning('Warning: trying to train a TCNN model with fewer than 20 datapoints.')

    regression_model = model.BaoRegressionModel(preprocessor)
    losses = regression_model.fit(x_train, y_train, x_test, y_test)
    regression_model.save(filename)

    return regression_model, losses


def _evaluate_prediction(y, predictions, plans, query_path, is_training) -> PerformancePrediction:
    default_plan = list(filter(lambda x: x.num_disabled_rules == 0, plans))[0]

    logger.info('y:\t%s', '\t'.join([f'{_:.2f}' for _ in y]))
    logger.info('ŷ:\t%s', '\t'.join(f'{prediction[0]:.2f}' for prediction in predictions))
    # the plan index which is estimated to perform best by Bao
    min_prediction_index = np.argmin(predictions)
    logger.info('min predicted index: %s (smaller is better)', str(min_prediction_index))

    # evaluate performance gains with Bao
    performance_from_model = y[min_prediction_index]
    logger.info('best choice -> %s', str(y[0] / default_plan.walltime))

    if performance_from_model < default_plan.walltime:
        logger.info('good choice -> %s', str(performance_from_model / default_plan.walltime))
    else:
        logger.info('bad choice -> %s', str(performance_from_model / default_plan.walltime))

    # The best **alternative** query plan is either the first or the second one
    best_alt_plan_walltime = plans[0].walltime if plans[0].num_disabled_rules > 0 else plans[1].walltime
    return PerformancePrediction(default_plan.walltime, plans[min_prediction_index].walltime, best_alt_plan_walltime, query_path, is_training)


def _choose_best_plans(query_plan_preprocessor, filename: str, test_configs: list[storage.Measurement], is_training: bool) -> list[PerformancePrediction]:
    """For each query, let the TCNN predict the performance of all query plans and compare them to the runtime of the default plan"""

    # load model
    bao_model = model.BaoRegressionModel(query_plan_preprocessor)
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
        plans_and_estimates = sorted(plans_and_estimates, key=lambda record: record.walltime)
        query_path = plans_and_estimates[0].query_path

        logger.info('Preprocess data for query %s', plans_and_estimates[0].query_path)
        x = [x.plan_json for x in plans_and_estimates]
        y = [x.walltime for x in plans_and_estimates]

        predictions = bao_model.predict(x)
        performance_prediction = _evaluate_prediction(y, predictions, plans_and_estimates, query_path, is_training)
        performance_predictions.append(performance_prediction)
    return list(reversed(sorted(performance_predictions, key=lambda entry: entry.selected_plan_relative_improvement)))


def train_tcnn(connector, bench: str, retrain: bool, create_datasets: bool):
    query_plan_preprocessor = connector.get_plan_preprocessor()()
    model_name = f'nn/model/{connector.get_name()}_model'
    data_path = f'nn/data/{connector.get_name()}_data'

    if create_datasets:
        x_train, y_train, x_test, y_test, training_data, test_data = _load_data(bench, training_ratio=0.85)
        _serialize_data(data_path, x_train, y_train, x_test, y_test, training_data, test_data)
    else:
        x_train, y_train, x_test, y_test, training_data, test_data = _deserialize_data(data_path)
        logger.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if retrain:
        _, (training_loss, test_loss) = _train_and_save_model(query_plan_preprocessor, model_name, x_train, y_train, x_test, y_test)
        plt.plot(range(len(training_loss)), training_loss, label='training')
        plt.plot(range(len(test_loss)), test_loss, label='test')
        plt.savefig(f'evaluation/losses_1dropout_{DROPOUT}.pdf')

    else:
        x_train, y_train, x_test, y_test, training_data, test_data = _deserialize_data(data_path)

    performance_test = _choose_best_plans(query_plan_preprocessor, model_name, test_data, is_training=False)
    performance_training = _choose_best_plans(query_plan_preprocessor, model_name, training_data, is_training=True)

    # calculate absolute improvements for test and training sets
    def calc_improvements(title: str, dataset: list):
        abs_runtime_bao = float(sum([x.selected_plan_runtime for x in dataset]))
        abs_runtime_best_hs = float(sum([x.best_alt_plan_runtime for x in dataset]))
        abs_runtime_test_default_plan = float(sum([x.default_plan_runtime for x in dataset]))

        results = f'----------------------------------------\n' \
                  f'{title}\n' \
                  f'----------------------------------------\n' \
                  f'Overall runtime of default plans: {abs_runtime_test_default_plan}\n' \
                  f'Overall runtime of bao selected plans: {abs_runtime_bao}\n' \
                  f'Overall runtime of best hs plans: {abs_runtime_best_hs}\n' \
                  f'Test improvement rel. w/ Bao: {1.0 - (abs_runtime_bao / float(abs_runtime_test_default_plan)):.4f}\n' \
                  f'Test improvement abs. w/ Bao: {abs_runtime_bao - abs_runtime_test_default_plan}\n' \
                  f'Test improvement rel. of best alternative hs: {1.0 - (abs_runtime_best_hs / float(abs_runtime_test_default_plan)):.4f}\n' \
                  f'Test improvement abs. of best alternative hs: {abs_runtime_best_hs - abs_runtime_test_default_plan}\n'
        return results

    with open(f'evaluation/results_{DROPOUT}.csv', 'a', encoding='utf-8') as f:
        f.write(calc_improvements('TEST SET', performance_test))
        f.write(calc_improvements('TRAINING SET', performance_training))
