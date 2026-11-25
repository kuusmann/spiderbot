import unittest
import time

import bot


class SpiderBotRuleTests(unittest.TestCase):
    def setUp(self):
        self.original_state = bot.state
        self.original_regime = bot.market_regime
        self.original_blacklist = set(bot.BLACKLISTED_TOKEN_ADDRESSES)
        bot.state = {
            "cash": 1000.0,
            "positions": {},
            "trades": [],
            "stopped_tokens": {},
            "signal_log": [],
            "price_history": {},
        }
        bot.market_regime = {"label": "NEUTRAL", "score": 50}

    def tearDown(self):
        bot.state = self.original_state
        bot.market_regime = self.original_regime
        bot.BLACKLISTED_TOKEN_ADDRESSES.clear()
        bot.BLACKLISTED_TOKEN_ADDRESSES.update(self.original_blacklist)

    def test_momentum_score_is_bounded(self):
        pair = {
            "txns": {"m5": {"buys": 80, "sells": 10}},
            "volume": {"m5": 60000},
            "liquidity": {"usd": 500000},
            "priceChange": {"m5": 4, "h1": 2},
            "_momentum": {"bounce_2m_pct": 3, "pool_age_minutes": 30},
        }
        score = bot.momentum_score(pair)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)

    def test_market_regime_detects_broad_positive_market(self):
        pairs = [
            {"liquidity": {"usd": 20000}, "volume": {"h24": 60000}, "priceChange": {"h1": 2}}
            for _ in range(5)
        ]
        regime = bot.market_regime_from_pairs(pairs)
        self.assertEqual(regime["label"], "BULL")
        self.assertEqual(regime["samples"], 5)

    def test_market_regime_unknown_when_no_eligible_data(self):
        regime = bot.market_regime_from_pairs([{"liquidity": {"usd": 100}, "volume": {"h24": 0}}])
        self.assertEqual(regime["label"], "UNKNOWN")

    def test_risk_assessment_adds_reported_risk_factors(self):
        pair = {"liquidity": {"usd": 9000}}
        report = {
            "score": 8,
            "lpLockedPct": 70,
            "creatorPct": 4,
            "topHolders": [{"pct": 5} for _ in range(6)],
        }
        risk = bot.risk_assessment(pair, report)
        self.assertGreater(risk["score"], 8)
        self.assertTrue(risk["factors"])
        self.assertIn(risk["level"], {"LOW", "MEDIUM", "HIGH"})

    def test_entry_risk_threshold_blocks_high_risk(self):
        self.assertTrue(bot.risk_entry_allowed({"score": 59.9}))
        self.assertFalse(bot.risk_entry_allowed({"score": 60.0}))
        self.assertFalse(bot.risk_entry_allowed({"score": 75}))
        self.assertFalse(bot.risk_entry_allowed({}))
        self.assertFalse(bot.risk_entry_allowed({"score": "invalid"}))
        self.assertFalse(bot.risk_entry_allowed({"score": 101}))

    def test_hard_stop_loss_exit(self):
        position = {"entry_price": 100, "peak_price": 100, "runner": False}
        reason, partial = bot.position_exit_decision(position, {}, 87, 60)
        self.assertTrue(reason.startswith("stop loss"))
        self.assertFalse(partial)

    def test_runner_takes_partial_profit(self):
        position = {"entry_price": 100, "peak_price": 140, "runner": True, "runner_half_sold": False}
        reason, partial = bot.position_exit_decision(position, {}, 140, 60)
        self.assertIn("runner partial TP", reason)
        self.assertTrue(partial)

    def test_runner_trailing_exit(self):
        position = {
            "entry_price": 100, "peak_price": 150, "runner": True,
            "runner_half_sold": True, "runner_peak_price": 150,
        }
        reason, partial = bot.position_exit_decision(position, {}, 115, 600)
        self.assertIn("runner trailing giveback", reason)
        self.assertFalse(partial)

    def test_scalp_profit_protection_trail(self):
        position = {"entry_price": 100, "peak_price": 130, "runner": False}
        reason, partial = bot.position_exit_decision(position, {}, 120, 600)
        self.assertEqual(reason, "adaptive profit-protection trail")
        self.assertFalse(partial)

    def test_blacklist_and_stop_cooldown(self):
        address = "test-token-address"
        bot.BLACKLISTED_TOKEN_ADDRESSES.add(address)
        blocked, reason = bot.token_entry_blocked(address)
        self.assertTrue(blocked)
        self.assertEqual(reason, "manual blacklist")
        bot.BLACKLISTED_TOKEN_ADDRESSES.remove(address)
        bot.state["stopped_tokens"][address] = time.time() + 60
        blocked, reason = bot.token_entry_blocked(address)
        self.assertTrue(blocked)
        self.assertIn("cooldown", reason)

    def test_position_size_respects_portfolio_exposure(self):
        bot.state["cash"] = 1000.0
        bot.state["positions"] = {"existing": {"cost": 480.0}}
        size = bot.dynamic_trade_size(1000.0, 0.0)
        self.assertGreaterEqual(size, 0)
        self.assertLessEqual(size, 20.0)

    def test_forward_signal_outcome_waits_for_future_observation(self):
        signal = {"signal_ts": 100.0, "price_usd": 2.0}
        history = [{"t": 1000.0, "p": 2.1}]
        self.assertIsNone(bot.evaluate_signal_forward(signal, history, 900, now=999))
        self.assertAlmostEqual(bot.evaluate_signal_forward(signal, history, 900, now=1000), 5.0)

    def test_user_requested_paper_limits_remain_set(self):
        self.assertEqual(bot.DAILY_STOP_LOSS_EUR, -40.0)
        self.assertEqual(bot.MAX_POSITIONS, 20)
        self.assertEqual(bot.MIN_MARKET_CAP_USD, 50000.0)


if __name__ == "__main__":
    unittest.main()
