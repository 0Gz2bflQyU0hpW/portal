SELECT
    videoid, domainid, platform, loghour, COUNT(1) AS hits
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    getVideoId(extstr) AS videoid,
                    getDomainId(oid) AS domainid,
                    getPlatform(fromcode) AS platform,
                    getHour(logtime) AS loghour
                FROM
                    logdata
            ) temp
        WHERE
            videoid IS NOT NULL
            AND domainid IS NOT NULL
            AND domainid IN ('1007088', '1007087', '1007086', '1007002')
            AND platform IS NOT NULL
            AND loghour IS NOT NULL
    ) playlog
GROUP BY
    videoid, domainid, platform, loghour