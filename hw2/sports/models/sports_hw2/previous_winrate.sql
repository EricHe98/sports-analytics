SELECT game_id,
	date,
	team,
	opposing_team,
	goal_differential,
	needle,
	cumulative_needle,
	COALESCE(LAG(cumulative_needle) OVER (PARTITION BY team, opposing_team ORDER BY date), 0) AS lagged_cumulative_teamwise_needle
FROM 
(
	SELECT game_id,
		date,
		team,
		opposing_team,
		goal_differential,
		CAST(CASE WHEN goal_differential > 0 THEN 1.0
			WHEN goal_differential = 0 THEN 0.0
			WHEN goal_differential < 0 THEN -1.0
			ELSE NULL END AS FLOAT) AS needle,
		AVG(CAST(CASE WHEN goal_differential > 0 THEN 1.0
			WHEN goal_differential = 0 THEN 0.0
			WHEN goal_differential < 0 THEN -1.0
			ELSE NULL END AS FLOAT)) OVER
			(PARTITION BY team, opposing_team ORDER BY date) AS cumulative_needle
	FROM {{ref('melted_view')}} s18
) a