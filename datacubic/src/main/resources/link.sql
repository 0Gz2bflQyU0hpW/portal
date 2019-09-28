select ua,count(*) as log_num,
    sum(case when result_code in ('0','-8108','-4501','-4098','-4010','-1019','-1018','-1011','-1010','-1008','-1007','-1005','-9','-105','-100','1','5','1014','9109','20003','20012','20015','20016','20017','20018','20019','20020','20021','20031','20034','20046','20101','20201','20130','20134','20135','20148','20156','20206','20208','20210','20603','107002','50112071','1001030042','1078030002') then 1 else 0 end) as succeed_num,
    sum(case when result_code in ('-1202','-1020','-1017','-1009','-1006','-1004','-1003','-1001','-200','-4','-1','2','6','22','54','205','302','303','307','400','403','404','405','500','502','503','504','705','706','1101','7002','7003','7004','105001','3020003') then 1 else 0 end) as neterr_num,
    sum(case when result_code in ('7','7001','3840','7005','8998','9102','20120','20205','201603','e000','3','4','1040002','3020005','3020006','10001','10009','10011') then 1 else 0 end) as localerr_num,
    sum(during_time) as during_time,
    sum(net_time) as net_time
from (
    select
        ua,result_code,during_time,net_time from fulllink
    where
        act='performance' and
        subtype='refresh_feed' and
        result_code is not null and
        during_time is not null and
        net_time is not null
)
group by ua





