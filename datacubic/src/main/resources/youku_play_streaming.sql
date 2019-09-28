SELECT
  *
FROM
  (
    SELECT
      getVideoId(extstr) AS videoid,
      getDomainId(oid) AS domainid,
      getPlatform(fromcode) AS platform,
      getTimestamp(logtime) AS timestamp
    FROM
      playlog
  ) temp
WHERE
  videoid IS NOT NULL
  AND domainid IS NOT NULL
  AND domainid IN (
    '1007088', '1007087', '1007086', '1007002'
  )
  AND platform IS NOT NULL
  AND timestamp IS NOT NULL