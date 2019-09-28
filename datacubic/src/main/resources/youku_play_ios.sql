SELECT
    *
FROM
    (
        SELECT
            extstr,
            getVideoId(extstr) AS videoid,
            oid,
            getDomainId(oid) AS domainid,
            fromcode,
            getPlatform(fromcode) AS platform,
            logtime,
            getHour(logtime) AS loghour
        FROM
            logdata
    ) temp
WHERE
    domainid IS NOT NULL
    AND domainid IN ('1007088', '1007087', '1007086', '1007002')
    AND platform IS NOT NULL
    AND platform = 'ios'