select sum(sn) total_num,
	sum(succ_num) succ_num,
	count(1) cou_num
from(
    select province,
        isp,
        dst_ip,
        cdn,
        error_code,
    	domain,
    	protocal,
    	count(1) sn,
    	sum(r_flag) succ_num
    from (
        select ipToLocation(ip)["province"] as province,
           ipToLocation(ip)["isp"] as isp,
           if(domain in ("wx1.sinaimg.cn","wx2.sinaimg.cn","wx3.sinaimg.cn","wx4.sinaimg.cn","ww1.sinaimg.cn",
           "ww2.sinaimg.cn","ww3.sinaimg.cn","ww4.sinaimg.cn","tva1.sinaimg.cn","tva2.sinaimg.cn","tva3.sinaimg.cn",
           "tva4.sinaimg.cn","tvax1.sinaimg.cn","tvax2.sinaimg.cn","tvax3.sinaimg.cn","tvax4.sinaimg.cn"),1,0) r_flag,
           dst_ip,
           cdn,
           error_code,
           domain,
           protocal
        from cdn_pic
    ) a0
    group by province,
        isp,
        dst_ip,
        cdn,
        error_code,
    	domain,
    	protocal
) b0