import numpy as np
import pandas as pd

from loader import stage_session


def summarize_session(player_ids, session_id, session_end, durations, country_map, duck_conn):
    """
    Assigns kills/deaths and stages session summary.
    """
    total_kills = np.random.randint(10, 60)
    total_deaths = total_kills  # balance

    kill_dist = np.random.multinomial(total_kills, np.random.dirichlet(np.ones(len(player_ids))))
    death_dist = np.random.multinomial(total_deaths, np.random.dirichlet(np.ones(len(player_ids))))

    summary_rows = []

    for i, pid in enumerate(player_ids):
        summary_rows.append({
            "playerId": pid,
            "sessionId": session_id,
            "eventDateTime": session_end.isoformat(),
            "country": country_map[pid],
            "eventLengthSeconds": durations[pid],
            "kills": kill_dist[i],
            "deaths": death_dist[i]
        })

    summary_df = pd.DataFrame(summary_rows)
    stage_session(summary_df, duck_conn)
