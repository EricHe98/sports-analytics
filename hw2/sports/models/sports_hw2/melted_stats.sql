SELECT s18.*,
	wr.lagged_cumulative_teamwise_needle,
	lf.lagged_cumulative_goal_differential,
	lf.lagged_cumulative_shots,
	lf.lagged_target_percent,
	lf.lagged_cumulative_needle,
	lf.games_played,
	COALESCE(pr1_16.pagerank_score, MIN(pr1_16.pagerank_score) OVER ()) AS team_pagerank_16,
	COALESCE(pr2_16.pagerank_score, MIN(pr2_16.pagerank_score) OVER ()) AS opposing_team_pagerank_16,
	COALESCE(pr1_17.pagerank_score, MIN(pr1_17.pagerank_score) OVER ()) AS team_pagerank_17,
	COALESCE(pr2_17.pagerank_score, MIN(pr2_17.pagerank_score) OVER ()) AS opposing_team_pagerank_17,
	CASE WHEN s18.outcome > 0 THEN 1 ELSE 0 END AS label,
	CASE WHEN s18.homeoraway = 'HomeTeam' THEN 1 ELSE 0 END AS is_home_team
FROM {{ref('melted_view')}} s18
JOIN {{ref('previous_winrate')}} wr 
	ON wr.date = s18.date
	AND wr.team = s18.team
JOIN {{ref('lagged_features')}} lf 
	ON lf.date = s18.date 
	AND lf.team = s18.team
LEFT JOIN {{source('sports', 'teams_pagerank_16')}} pr1_16
	ON pr1_16.team_id = s18.team_id 
LEFT JOIN {{source('sports', 'teams_pagerank_16')}} pr2_16 
	ON pr2_16.team_id = s18.opposing_team_id
LEFT JOIN {{source('sports', 'teams_pagerank_17')}} pr1_17
	ON pr1_17.team_id = s18.team_id 
LEFT JOIN {{source('sports', 'teams_pagerank_17')}} pr2_17 
	ON pr2_17.team_id = s18.opposing_team_id