from datetime import timedelta
import numpy as np

from src.utils import HEARTBEAT_INTERVAL


def simulate_heartbeats(player_ids, session_id, team_ids, session_start, speed_map, durations):
    """
    Simulates heartbeat logs per player using Lorentz-style motion.
    """
    heartbeats = []

    for pid in player_ids:
        duration = durations[pid]
        speed = speed_map[pid]
        num_beats = duration // HEARTBEAT_INTERVAL

        # Random initial position
        x, y, z = np.random.uniform(-100, 100, size=3)

        for i in range(num_beats):
            ts = session_start + timedelta(seconds=i * HEARTBEAT_INTERVAL)

            # Simple Lorentz-like motion along x-axis
            dx = speed / (1 + x**2)
            x += dx
            y += np.sin(i / 10.0) * speed * 0.1
            z += np.cos(i / 10.0) * speed * 0.1

            # Clip to grid
            x = max(min(x, 100), -100)
            y = max(min(y, 100), -100)
            z = max(min(z, 100), -100)

            heartbeats.append({
                "timestamp": ts.isoformat(),
                "player_id": pid,
                "session_id": session_id,
                "x": round(x, 3),
                "y": round(y, 3),
                "z": round(z, 3)
            })

    return heartbeats
