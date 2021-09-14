from shutil import rmtree

import tensorflow as tf
from tf_agents.agents.dqn.dqn_agent import DqnAgent
from tf_agents.drivers.dynamic_step_driver import DynamicStepDriver
from tf_agents.environments.tf_py_environment import TFPyEnvironment
from tf_agents.networks.q_network import QNetwork
from tf_agents.replay_buffers.tf_uniform_replay_buffer import TFUniformReplayBuffer
from tf_agents.utils.common import element_wise_squared_loss
from unittest import TestCase

from trading.datamodel.environment.match_odds_environment import MatchOddsEnvironment
from trading.datamodel.odds_series.match_odds_series import MatchOddsSeries
from trading.datamodel.outcomes.match_outcome import MatchOutcome
from trading.tests.test_datamodel.constants import MatchOddsTestData
from trading.internal.reinforcement_model import ReinforcementModel
from utils.paths import get_path


class TestReinforcementModel(TestCase):
    def __init__(self, methodName="runTest"):
        super().__init__(methodName=methodName)
        self.inactionPenalty = -1
        self.testSavePath = get_path("./test_rl_model")

    def setUp(self):
        super().setUp()
        self.exampleMatchOddsSeries = MatchOddsSeries(
            oddsDataframe=MatchOddsTestData.testDataframe,
            doCycle=False,
            matchOutcome=MatchOutcome.HOME_WIN,
            backScalingFactor=MatchOddsTestData.backScalingFactor,
        )
        self.matchOddsEnvironment = MatchOddsEnvironment(
            oddSeries=self.exampleMatchOddsSeries,
            rewardDiscountFactor=0.1,
            inactionPenalty=self.inactionPenalty,
            onlyPositiveCashout=False,
        )
        trainingEnvironment = TFPyEnvironment(self.matchOddsEnvironment)
        evaluationEnvironment = TFPyEnvironment(self.matchOddsEnvironment)
        trainStepCounter = tf.Variable(0)
        agent = DqnAgent(
            time_step_spec=trainingEnvironment.time_step_spec(),
            action_spec=trainingEnvironment.action_spec(),
            q_network=QNetwork(
                input_tensor_spec=trainingEnvironment.observation_spec(),
                action_spec=trainingEnvironment.action_spec(),
                fc_layer_params=(
                    12,
                    6,
                ),
                dropout_layer_params=(
                    0.1,
                    0.1,
                ),
            ),
            optimizer=tf.compat.v1.train.AdamOptimizer(learning_rate=1e-5),
            td_errors_loss_fn=element_wise_squared_loss,
            gradient_clipping=10.0,
            train_step_counter=trainStepCounter,
            observation_and_action_constraint_splitter=self.matchOddsEnvironment.get_action_mask,
        )
        replayBuffer = TFUniformReplayBuffer(
            data_spec=agent.collect_data_spec, batch_size=trainingEnvironment.batch_size, max_length=int(1e2)
        )
        collectDriver = DynamicStepDriver(
            env=trainingEnvironment, policy=agent.collect_policy, observers=[replayBuffer.add_batch], num_steps=10
        )
        self.model = ReinforcementModel(
            agent=agent,
            trainingEnvironment=trainingEnvironment,
            trainingBatchSize=3,
            evaluationEnvironment=evaluationEnvironment,
            evaluationBatchSize=3,
            replayBuffer=replayBuffer,
            collectDriver=collectDriver,
            outputDirectory=self.testSavePath,
        )

    def tearDown(self):
        # rmtree(path=self.testSavePath)
        pass

    def test_initialise(self):
        # TODO(MIKE): need to fix action spec mask function - try and pass in tuple as observation, to make processing easier?
        self.model.initialize()
        self.assertIsNotNone(self.model.trainingDataset)
        self.assertIsNotNone(self.model.policySaver)
        self.assertIsNotNone(self.model.trainingSummaryWriter)
        self.assertIsNotNone(self.model.evaluationSummaryWriter)

    # def test_train(self):
    #     pass
    #
    # def test_evaluate(self):
    #     pass
    #
    # def test_compute_return_for_episodes(self):
    #     pass
    #
    # def test_produce_metrics(self):
    #     pass
    #
    # def test_checkpoint(self):
    #     pass
    #
    # def test_train_model(self):
    #     pass
