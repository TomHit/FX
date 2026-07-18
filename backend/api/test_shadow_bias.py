import copy
import unittest

from shadow_bias_v2 import (
    build_structure_context,
    classify_structure,
    compute_shadow_bias,
    detect_choch,
    evaluate_liquidity_evidence,
    evaluate_zone_evidence,
)


def bars_from_points(points, count=80, start=1700000000, step=3600, scale=0.15):
    """Interpolate a pivot path into valid OHLC candles."""
    closes=[]
    segs=max(1, len(points)-1)
    per=max(2, count//segs)
    for a,b in zip(points[:-1], points[1:]):
        for j in range(per):
            t=j/per
            closes.append(a+(b-a)*t)
    closes.append(points[-1])
    while len(closes)<count:
        closes.append(closes[-1])
    closes=closes[:count]
    out=[]
    prev=closes[0]
    for i,c in enumerate(closes):
        o=prev
        out.append({"t":start+i*step,"o":o,"h":max(o,c)+scale,"l":min(o,c)-scale,"c":c,"complete":True})
        prev=c
    return out


def bullish_bars(count=84, step=3600):
    return bars_from_points([100,106,102,110,105,114,108,118,112],count=count,step=step)


def bearish_bars(count=84, step=3600):
    return bars_from_points([120,114,118,110,115,106,111,102,107],count=count,step=step)


def range_bars(count=84, step=3600):
    return bars_from_points([100,105,99,104,100,105,99.5,104.5,100],count=count,step=step)


def sr_bundle(price=113):
    return {
        "best_support": {"level":111,"low":110.5,"high":111.5,"side_ok":True,"stale":False,"broken":False,"quality_score":42,"htf_confluence":True,"swept":True,"reclaimed":True,"evidence":{"reaction_atr":1.5,"ob_overlap":True,"liq_pool":True}},
        "best_resistance": {"level":116,"low":115.5,"high":116.5,"side_ok":True,"stale":False,"broken":False,"quality_score":18,"htf_confluence":False,"evidence":{"reaction_atr":0.5}},
        "active_supports": [], "active_resistances": [],
    }


class ShadowBiasV2Tests(unittest.TestCase):
    def test_unknown_on_insufficient_data(self):
        b=bullish_bars(10)
        r=compute_shadow_bias(symbol="X",bars_h1=b,bars_h4=b,price=110)
        self.assertEqual(r["shadow_bias"],"UNKNOWN")
        self.assertFalse(r["shadow_bias_data_ok"])

    def test_bullish_structure(self):
        ctx=build_structure_context(bullish_bars())
        self.assertEqual(ctx["major"]["state"],"BULLISH")

    def test_bearish_structure(self):
        ctx=build_structure_context(bearish_bars())
        self.assertEqual(ctx["major"]["state"],"BEARISH")

    def test_structure_classifier_mixed(self):
        s={"highs":[{"level":10},{"level":12}],"lows":[{"level":8},{"level":7}]}
        self.assertEqual(classify_structure(s)["state"],"EXPANDING_MIXED")

    def test_wick_only_choch_rejected(self):
        b=bearish_bars()
        ctx=build_structure_context(b)
        pivot=ctx["major"]["last_high"]["level"]
        atr=ctx["atr"]
        b[-1]=dict(b[-1],h=pivot+atr*0.3,c=pivot-atr*0.05,l=min(b[-1]["l"],pivot-atr*0.2))
        out=detect_choch(b,ctx)
        self.assertEqual(out["state"],"NONE")
        self.assertTrue(out["wick_only_rejected"])

    def test_close_confirmed_bullish_choch(self):
        b=bearish_bars()
        ctx=build_structure_context(b)
        pivot=ctx["major"]["last_high"]["level"]
        atr=ctx["atr"]
        c=pivot+atr*0.25
        b[-1]=dict(b[-1],h=c+atr*0.1,c=c,l=min(b[-1]["l"],c-atr*0.2))
        out=detect_choch(b,ctx)
        self.assertEqual(out["state"],"BULLISH_CHOCH")

    def test_liquidity_buy_confirmation(self):
        liq={"bsl_ssl":{"ssl":{"swept":True,"reaction_after_sweep":1.2},"bsl":{}},"signals":["BULLISH_RECLAIM"]}
        out=evaluate_liquidity_evidence(liq)
        self.assertEqual(out["state"],"CONFIRM_BUY")
        self.assertGreater(out["buy_score"],out["sell_score"])

    def test_liquidity_conflict(self):
        liq={"bsl_ssl":{"ssl":{"swept":True,"reaction_after_sweep":1},"bsl":{"swept":True,"reaction_after_sweep":1}}}
        self.assertEqual(evaluate_liquidity_evidence(liq)["state"],"CONFLICT")

    def test_zone_wrong_side_rejected(self):
        sr={"best_support":{"level":120,"low":119,"high":121,"quality_score":50,"side_ok":True}}
        out=evaluate_zone_evidence(sr,"BUY",110,2)
        self.assertFalse(out["valid"])

    def test_zone_stale_rejected(self):
        sr={"best_support":{"level":108,"low":107,"high":109,"quality_score":50,"side_ok":True,"stale":True}}
        out=evaluate_zone_evidence(sr,"BUY",110,2)
        self.assertFalse(out["valid"])

    def test_actionable_boundary(self):
        sr={"best_support":{"level":106,"low":105,"high":106,"quality_score":30,"side_ok":True,"stale":False,"broken":False}}
        self.assertTrue(evaluate_zone_evidence(sr,"BUY",110,2,actionable_atr=2.0)["actionable"])

    def test_aligned_buy_bias(self):
        h1=bullish_bars()
        h4=bullish_bars(step=14400)
        liq={"bsl_ssl":{"ssl":{"swept":True,"reaction_after_sweep":2.0},"bsl":{}},"signals":["BULLISH_RECLAIM"]}
        r=compute_shadow_bias(symbol="XAUUSD",bars_h1=h1,bars_h4=h4,price=113,sr_bundle=sr_bundle(),liquidity_context=liq,executed_side="BUY",computed_ms=123)
        self.assertEqual(r["shadow_bias"],"BUY")
        self.assertEqual(r["shadow_bias_relation"],"ALIGNED")
        self.assertGreater(r["buy_score"],r["sell_score"])

    def test_conflicting_timeframes_not_high_confidence(self):
        r=compute_shadow_bias(symbol="X",bars_h1=bullish_bars(),bars_h4=bearish_bars(step=14400),price=113,sr_bundle=sr_bundle(),computed_ms=123)
        self.assertNotEqual(r["shadow_bias_confidence"],"HIGH")
        self.assertIn("H1_H4_MAJOR_CONFLICT",r.get("conflicts",[]))

    def test_neutral_when_edge_small(self):
        sr={
            "best_support":{"level":109,"low":108.5,"high":109.5,"quality_score":25,"side_ok":True,"stale":False,"broken":False},
            "best_resistance":{"level":111,"low":110.5,"high":111.5,"quality_score":25,"side_ok":True,"stale":False,"broken":False},
        }
        r=compute_shadow_bias(symbol="X",bars_h1=range_bars(),bars_h4=range_bars(step=14400),price=110,sr_bundle=sr,computed_ms=123)
        self.assertIn(r["shadow_bias"],{"NEUTRAL","UNKNOWN"})

    def test_deterministic_payload(self):
        kwargs=dict(symbol="X",bars_h1=bullish_bars(),bars_h4=bullish_bars(step=14400),price=113,sr_bundle=sr_bundle(),computed_ms=999)
        self.assertEqual(compute_shadow_bias(**copy.deepcopy(kwargs)),compute_shadow_bias(**copy.deepcopy(kwargs)))

    def test_forming_bar_ignored(self):
        h1=bullish_bars(); h4=bullish_bars(step=14400)
        base=compute_shadow_bias(symbol="X",bars_h1=h1,bars_h4=h4,price=113,sr_bundle=sr_bundle(),computed_ms=1)
        h1.append({"t":9999999999,"o":113,"h":200,"l":50,"c":60,"complete":False})
        after=compute_shadow_bias(symbol="X",bars_h1=h1,bars_h4=h4,price=113,sr_bundle=sr_bundle(),computed_ms=1)
        self.assertEqual(base,after)


if __name__ == "__main__":
    unittest.main()
