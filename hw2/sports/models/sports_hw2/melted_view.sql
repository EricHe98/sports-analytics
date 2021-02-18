SELECT s.*,
	CASE WHEN goal_differential > 0 THEN 1
		WHEN goal_differential = 0 THEN 0
		WHEN goal_differential < 0 THEN -1
		ELSE NULL END AS outcome
FROM 
(
	SELECT s18.*,
		CASE WHEN s18.homeoraway = 'HomeTeam' 
			THEN s18.fthg - s18.ftag 
			WHEN s18.homeoraway = 'AwayTeam'
			THEN s18.ftag - s18.fthg 
			END AS goal_differential,
		t1.team_id,
		s18_other.team AS opposing_team,
		t2.team_id AS opposing_team_id
	FROM {{ source('sports', 'sports18_melted')}} s18 
	JOIN {{ source('sports', 'sports18_melted')}} s18_other
		on s18.game_id = s18_other.game_id
		and s18.team != s18_other.team
	JOIN {{ref('teams')}} t1 
		ON t1.team = s18.team 
	JOIN {{ref('teams')}} t2 
		ON t2.team = s18_other.team
		) s
