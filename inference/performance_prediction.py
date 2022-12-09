"""This module implements a wrapper class for performance predictions from the TCNN, which is used for visualizations and plots."""


class PerformancePrediction:
    """Class to store the performance predictions from Bao for a certain query and all the generated QEPs for further evaluation"""

    def __init__(self, default_plan_runtime, selected_plan_runtime, best_alt_plan_runtime, query_path, is_training_sample=False):
        self.default_plan_runtime = default_plan_runtime
        self.selected_plan_runtime = selected_plan_runtime  # the plan that has the estimated best runtime (using Bao)
        self.best_alt_plan_runtime = best_alt_plan_runtime  # the overall best alternative configuration for this query, validated by experiment
        self.query_path = query_path
        # Performance improvements for the by Bao selected plan and the best plan wrt. the default plan
        self.selected_plan_relative_improvement = (float(default_plan_runtime) - float(selected_plan_runtime)) / float(default_plan_runtime)
        self.selected_plan_absolute_improvement = (float(default_plan_runtime) - float(selected_plan_runtime))
        self.best_plan_relative_improvement = (float(default_plan_runtime) - float(best_alt_plan_runtime)) / float(default_plan_runtime)
        self.best_plan_absolute_improvement = (float(default_plan_runtime) - float(best_alt_plan_runtime))
        self.is_training_sample = is_training_sample
