from typing import Iterable, Tuple, Union
from pathlib import Path

import numpy as np
import tensorflow as tf
from tf_agents.agents import TFAgent
from tf_agents.drivers.driver import Driver
from tf_agents.environments.tf_py_environment import TFPyEnvironment
from tf_agents.replay_buffers.replay_buffer import ReplayBuffer
from tf_agents.policies.policy_saver import PolicySaver
from tf_agents.utils import common

from trading.datamodel.environment.base_environment import BaseEnvironment
from utils.paths import get_path


class ReinforcementModel:
    def __init__(
        self,
        agent: TFAgent,
        trainingEnvironment: TFPyEnvironment,
        trainingBatchSize: int,
        evaluationEnvironment: TFPyEnvironment,
        evaluationBatchSize: int,
        replayBuffer: ReplayBuffer,
        collectDriver: Driver,
        outputDirectory: Union[str, Path],
    ):
        self.agent = agent
        self.agent.train = common.function(agent.train)  # wrapping for optimization of graph in training
        self.agent.train_step_counter.assign(0)
        self.trainingEnvironment = trainingEnvironment
        self.trainingBatchSize = trainingBatchSize
        self.evaluationEnvironment = evaluationEnvironment
        self.evaluationBatchSize = evaluationBatchSize
        self.replayBuffer = replayBuffer
        self.collectDriver = collectDriver
        self.outputDirectory = outputDirectory
        self.tensorboardPath = get_path(outputDirectory, "tensorboard")
        self.checkpointDirectory = get_path(outputDirectory, "checkpoints")
        self.globalStep = 0
        self.trainingDataset = None
        self.policySaver = None
        self.trainingSummaryWriter = None
        self.evaluationSummaryWriter = None

    def _get_training_dataset(self) -> Iterable[tf.Tensor]:
        return iter(
            self.replayBuffer.as_dataset(
                num_parallel_calls=3,
                sample_batch_size=self.trainingBatchSize,
                num_steps=self.agent._n_step_update + 1,
            ).prefetch(3)
        )

    def initialize(self) -> None:
        self.trainingDataset = self._get_training_dataset()
        self.policySaver = PolicySaver(policy=self.agent.policy)
        self.trainingSummaryWriter = tf.summary.create_file_writer(logdir=get_path(self.tensorboardPath, "train"))
        self.evaluationSummaryWriter = tf.summary.create_file_writer(logdir=get_path(self.tensorboardPath, "evaluate"))

    def train(self) -> tf.Tensor:
        for _ in range(self.collectDriver._num_steps):
            self.collectDriver.run()
        experience, _ = next(self.trainingDataset)
        trainLoss = self.agent.train(experience=experience).loss
        tf.summary.trace_on(graph=True, profiler=False)
        with self.trainingSummaryWriter.as_default():
            tf.summary.scalar(name="Training Loss", data=trainLoss, step=self.globalStep)
            tf.summary.trace_export(
                name=f"{self.__class__.__name__}{self.globalStep}",
                step=self.globalStep,
                profiler_outdir=get_path(self.tensorboardPath, "trainGraph"),
            )
        self.globalStep += 1
        return trainLoss

    def evaluate(self) -> None:
        self.produce_metrics()

    def compute_return_for_episodes(self, environment: BaseEnvironment) -> Tuple[tf.Tensor, tf.Tensor, tf.Tensor]:
        allEpisodeReturns = []
        allActions = []
        allMaskedReturns = []
        for _ in range(self.evaluationBatchSize):
            timeStep = environment.reset()
            episodeReturns = []
            episodeActions = []
            while not timeStep.is_last():
                actionStep = self.agent.policy.action(time_step=timeStep)
                episodeActions.append(actionStep.action)
                timeStep = environment.step(action=actionStep.action)
                episodeReturns.append(timeStep.reward)

            allEpisodeReturns.append(tf.expand_dims(tf.reduce_sum(episodeReturns), axis=-1))
            allActions.extend(episodeActions)
            maskedReturns = tf.boolean_mask(tensor=episodeReturns, mask=tf.concat(episodeActions, axis=-1) != 0)
            allMaskedReturns.append(tf.expand_dims(tf.reduce_sum(maskedReturns), axis=-1))
        return tf.concat(allEpisodeReturns, axis=-1), tf.concat(allActions, axis=-1), tf.concat(allMaskedReturns, axis=-1)

    def produce_metrics(self) -> None:
        tf.summary.trace_on(graph=True, profiler=False)
        with self.evaluationSummaryWriter.as_default():
            episodeReturns, allActions, allMaskedReturns = self.compute_return_for_episodes(
                environment=self.evaluationEnvironment
            )
            maskedActions = tf.boolean_mask(tensor=allActions, mask=allActions != 0)
            tf.summary.histogram(name="allActions", data=maskedActions, step=self.globalStep)
            tf.summary.scalar(name="numActions", data=len(maskedActions), step=self.globalStep)

            tf.summary.histogram(name="unmaskedEpisodeReturns", data=episodeReturns, step=self.globalStep)
            tf.summary.scalar(
                name=f"averageUnmaskedReturnOver{self.evaluationBatchSize}Episodes",
                data=tf.reduce_mean(episodeReturns),
                step=self.globalStep,
            )

            tf.summary.histogram(name="maskedEpisodeReturns", data=allMaskedReturns, step=self.globalStep)
            averageMaskedReturn = tf.reduce_mean(allMaskedReturns)
            tf.summary.scalar(
                name=f"averageMaskedReturnOver{self.evaluationBatchSize}Episodes",
                data=averageMaskedReturn,
                step=self.globalStep,
            )

            totalPortfolioReturn = tf.reduce_sum(allMaskedReturns)
            sharpeRatio = totalPortfolioReturn / tf.math.reduce_std(allMaskedReturns)
            tf.summary.scalar(name="sharpeRatio", data=sharpeRatio, step=self.globalStep)
            sortinoRatio = totalPortfolioReturn / tf.math.reduce_std(
                tf.boolean_mask(tensor=allMaskedReturns, mask=allMaskedReturns < 0)
            )
            tf.summary.scalar(name="sortinoRatio", data=sortinoRatio, step=self.globalStep)

            significance = (averageMaskedReturn / tf.math.reduce_std(allMaskedReturns)) * np.sqrt(self.evaluationBatchSize)
            tf.summary.scalar(name="significance", data=significance, step=self.globalStep)

            tf.summary.trace_export(
                name=f"{self.__class__.__name__}{self.globalStep}",
                step=self.globalStep,
                profiler_outdir=get_path(self.tensorboardPath, "evaluateGraph"),
            )

    def checkpoint(self) -> None:
        checkpointer = common.Checkpointer(
            ckpt_dir=get_path(self.checkpointDirectory, self.globalStep, "checkpoints"),
            max_to_keep=1,
            agent=self.agent,
            policy=self.agent.policy,
            replay_buffer=self.replayBuffer,
            global_step=self.agent.train_step_counter,
        )
        checkpointer.save(global_step=self.globalStep)
        self.policySaver.save(export_dir=get_path(self.checkpointDirectory, self.globalStep, "policy"))

    def train_model(self, numSteps: int, evaluationFrequency: int) -> None:
        for step in range(numSteps):
            if step % evaluationFrequency == 0:
                self.evaluate()
                self.checkpoint()
            else:
                self.train()
