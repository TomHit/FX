# /opt/xauapi/api/prop_firms/prop_config.py

PROP_FIRM_RULES = {
    "ftmo": {
        "label": "FTMO",
        "phases": {
            "challenge": {
                "target_pct": 10,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": 4,
                "risk_per_idea_pct": None,
            },
            "verification": {
                "target_pct": 5,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": 4,
                "risk_per_idea_pct": None,
            },
            "funded": {
                "target_pct": None,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": None,
                "risk_per_idea_pct": None,
            },
        },
    },

    "fundingpips": {
        "label": "FundingPips",
        "phases": {
            "phase_1_8": {
                "target_pct": 8,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": 3,
                "risk_per_idea_pct": None,
            },
            "phase_1_10": {
                "target_pct": 10,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": 3,
                "risk_per_idea_pct": None,
            },
            "phase_2": {
                "target_pct": 5,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": 3,
                "risk_per_idea_pct": None,
            },
            "funded": {
                "target_pct": None,
                "daily_loss_pct": 5,
                "max_loss_pct": 10,
                "min_days": None,
                "risk_per_idea_pct": 3,
            },
        },
    },
}

SYMBOL_SPECS = {
    "XAUUSD": {"contract_size": 100, "lot_step": 0.01, "min_lot": 0.01},
    "EURUSD": {"contract_size": 100000, "lot_step": 0.01, "min_lot": 0.01},
    "GBPUSD": {"contract_size": 100000, "lot_step": 0.01, "min_lot": 0.01},
    "USDCAD": {"contract_size": 100000, "lot_step": 0.01, "min_lot": 0.01},
    "USDCHF": {"contract_size": 100000, "lot_step": 0.01, "min_lot": 0.01},
    "USDJPY": {"contract_size": 100000, "lot_step": 0.01, "min_lot": 0.01},
}