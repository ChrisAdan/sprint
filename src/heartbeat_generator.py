from datetime import timedelta
import numpy as np

from utils import HEARTBEAT_INTERVAL, GRID_BOUNDS
from movement.step import lorentzian, bezier, lissajous, perlin


STEP_FUNCTIONS = {
    "lorentzian": lorentzian.step,
    "bezier": bezier.step,
    "lissajous": lissajous.step,
    "perlin": perlin.step
}


def clamp_to_bounds(x, y, z):
    lower, upper = GRID_BOUNDS
    x = max(min(x, upper), lower)
    y = max(min(y, upper), lower)
    z = max(min(z, upper), lower)
    return x, y, z


def simulate_heartbeats(player_ids, session_id, team_ids, session_start, speed_map, durations, behavior_map):
    """
    Simulates heartbeats for all players using assigned movement types.

    Returns a list of heartbeat dicts with XYZ positions every 30s.
    """
    heartbeats = []
    positions = {}

    # Assign unique starting positions
    attempts = 0
    while len(positions) < len(player_ids):
        x, y, z = np.random.uniform(*GRID_BOUNDS, size=3)
        collision = any(np.linalg.norm(np.array([x, y, z]) - np.array(pos)) < 1
                        for pos in positions.values())
        if not collision:
            pid = player_ids[len(positions)]
            positions[pid] = (x, y, z)
        else:
            attempts += 1
            if attempts > 1000:
                raise ValueError("Unable to place players without collisions.")

    # Simulate each player's movement
    for pid in player_ids:
        duration = durations[pid]
        speed = speed_map[pid]
        team = team_ids[pid]
        behavior = behavior_map[pid]
        step_fn = STEP_FUNCTIONS[behavior]

        x, y, z = positions[pid]
        num_beats = duration // HEARTBEAT_INTERVAL

        for i in range(num_beats):
            ts = session_start + timedelta(seconds=i * HEARTBEAT_INTERVAL)

            x, y, z = step_fn(x, y, z, speed, i)
            x, y, z = clamp_to_bounds(x, y, z)

            heartbeats.append({
                "timestamp": ts.isoformat(),
                "playerId": pid,
                "sessionId": session_id,
                "teamId": team,
                "positionX": round(x, 3),
                "positionY": round(y, 3),
                "positionZ": round(z, 3)
            })

    return heartbeats
