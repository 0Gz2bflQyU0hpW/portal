SELECT country,province,city,isp,ua,ap,version,cdn,duration_type,COUNT(1) AS hits
  FROM (SELECT ipToLocation(ip)["country"] as country,
               ipToLocation(ip)["province"] as province,
               ipToLocation(ip)["city"] as city,
               ipToLocation(ip)["isp"] as isp,
               ua,ap,version,cdn,
               (CASE WHEN video_firstframe_time>0 AND video_firstframe_time<=1000 THEN '0-1s'
                     WHEN video_firstframe_time>1000 AND video_firstframe_time<=2000 THEN '1-2s'
                     WHEN video_firstframe_time>2000 AND video_firstframe_time<=4000 THEN '2-4s'
                     WHEN video_firstframe_time>4000 AND video_firstframe_time<=5000 THEN '4-5s'
                     WHEN video_firstframe_time>5000 THEN '>5s'
                     ELSE '0' END) AS duration_type
          FROM download_slow
        WHERE video_url=1 AND video_firstframe_time>=0 AND video_firstframe_time<=50000 AND version>='6.0.0') t1
GROUP BY country,province,city,isp,ua,ap,version,cdn,duration_type


