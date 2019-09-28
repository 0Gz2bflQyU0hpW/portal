select sum(sn) total_num,
	sum(succ_num) succ_num,
	count(1) cou_num
from(
    select province,
        isp,
        dst_ip,
        cdn,
        video_firstframe_status,
    	domain,
    	protocal,
    	count(1) sn,
    	sum(r_flag) succ_num
    from (
        select ipToLocation(ip)["province"] as province,
           ipToLocation(ip)["isp"] as isp,
           if(domain in ("f.us.sinaimg.cn",
				"us.sinaimg.cn",
				"g.us.sinaimg.cn",
				"hd.us.sinaimg.cn",
				"s.us.sinaimg.cn",
				"v.us.sinaimg.cn",
				"wbvip.us.sinaimg.cn",
				"locallimit.us.sinaimg.cn",
				"s3.us.sinaimg.cn",
				"mp.us.sinaimg.cn",
				"public.us.sinaimg.cn")
			   and video_cache_type = "0",
			1,0) r_flag,
           dst_ip,
           cdn,
           video_firstframe_status,
           domain,
           protocal
        from cdn_video
    ) a0
    group by province,
        isp,
        dst_ip,
        cdn,
        video_firstframe_status,
    	domain,
    	protocal
) b0