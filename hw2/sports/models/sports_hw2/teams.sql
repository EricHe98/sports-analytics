SELECT team,
	ROW_NUMBER() OVER (ORDER BY team) AS team_id
FROM
(
	SELECT DISTINCT team
	FROM {{source('sports', 'sports18_melted')}} s18
) t