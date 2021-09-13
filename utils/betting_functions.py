def calculate_back_bet_to_trade_out(layOdds, layStake, currentBackOdds):
    return (layOdds * layStake) / currentBackOdds


def calculate_lay_bet_to_trade_out(backOdds, backStake, currentLayOdds):
    return (backOdds * backStake) / currentLayOdds
