# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Implementation of tree convolutional neural networks"""
import torch
from torch import nn


class BinaryTreeConv(nn.Module):
    """We can think of the tree conv as a single dense layer that we 'drag' across the tree."""

    def __init__(self, in_channels, out_channels):
        super().__init__()

        self.__in_channels = in_channels
        self.__out_channels = out_channels
        self.weights = nn.Conv1d(in_channels, out_channels, stride=3, kernel_size=3)

    def forward(self, flat_data):
        trees, idxes = flat_data
        orig_idxes = idxes
        idxes = idxes.expand(-1, -1, self.__in_channels).transpose(1, 2)
        expanded = torch.gather(trees, 2, idxes)

        results = self.weights(expanded)

        # Add a zero vector back on
        zero_vec = torch.zeros((trees.shape[0], self.__out_channels)).unsqueeze(2)
        zero_vec = zero_vec.to(results.device)
        results = torch.cat((zero_vec, results), dim=2)
        return (results, orig_idxes)


class TreeActivation(nn.Module):
    def __init__(self, activation):
        super().__init__()
        self.activation = activation

    def forward(self, x):
        return self.activation(x[0]), x[1]


class TreeLayerNorm(nn.Module):
    def forward(self, x):
        data, idxes = x
        mean = torch.mean(data, dim=(1, 2)).unsqueeze(1).unsqueeze(1)
        std = torch.std(data, dim=(1, 2)).unsqueeze(1).unsqueeze(1)
        normd = (data - mean) / (std + 0.00001)
        return (normd, idxes)


class DynamicPooling(nn.Module):
    def forward(self, x):
        return torch.max(x[0], dim=2).values
