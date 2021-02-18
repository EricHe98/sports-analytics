SELECT game_id, team, date, homeoraway, goal_differential, 
	cumulative_goal_differential,
	COALESCE(LAG(cumulative_goal_differential) OVER (PARTITION BY team ORDER BY date), 0) AS lagged_cumulative_goal_differential,
	COALESCE(LAG(avg_shots) OVER (PARTITION BY team ORDER BY date), 0) AS lagged_cumulative_shots,
	CASE WHEN cumulative_shots > 0 
		THEN COALESCE(LAG(cumulative_target / cumulative_shots) OVER (PARTITION BY team ORDER BY date), 0)
		WHEN cumulative_shots = 0 
		THEN 0
		ELSE NULL END AS lagged_target_percent,
	COALESCE(LAG(cumulative_needle) OVER (PARTITION BY team ORDER BY date), 0) AS lagged_cumulative_needle,
	games_played
FROM 
(
	SELECT game_id, team, date, homeoraway, 
		goal_differential,
		AVG(goal_differential) OVER (PARTITION BY team ORDER BY date) AS cumulative_goal_differential,
		AVG(CASE WHEN homeoraway = 'HomeTeam' THEN s18.hs
				WHEN homeoraway = 'AwayTeam' THEN s18.as 
				ELSE NULL END) OVER (PARTITION BY team ORDER BY date) AS avg_shots,
		SUM(CASE WHEN homeoraway = 'HomeTeam' THEN s18.hs
				WHEN homeoraway = 'AwayTeam' THEN s18.as 
				ELSE NULL END) OVER (PARTITION BY team ORDER BY date) AS cumulative_shots,
		SUM(CASE WHEN homeoraway = 'HomeTeam' THEN s18.hst
				WHEN homeoraway = 'AwayTeam' THEN s18.ast
				ELSE NULL END) OVER (PARTITION BY team ORDER BY date) AS cumulative_target,
		AVG(CAST(CASE WHEN goal_differential > 0 THEN 1 
			WHEN goal_differential = 0 THEN 0
			WHEN goal_differential < 0 THEN -1 
			ELSE NULL END AS FLOAT))
			OVER (PARTITION BY team ORDER BY date) AS cumulative_needle,
		ROW_NUMBER() OVER (PARTITION BY team ORDER BY date) - 1 AS games_played
	FROM {{ ref('melted_view') }} s18 
) cgd