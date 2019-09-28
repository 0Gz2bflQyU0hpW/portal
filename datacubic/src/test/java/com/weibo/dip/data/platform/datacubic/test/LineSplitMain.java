package com.weibo.dip.data.platform.datacubic.test;

/**
 * Created by yurun on 17/5/8.
 */
public class LineSplitMain {

    public static void main(String[] args) {
        String line = "/data1/app_bindrequestdev7g6a8l_dnslog;/data1/app_bindrequestdev7g6a8l_querylog;/data1/app_comment239n7zjqyjbi8_allnews;/data1/app_comment239n7zjqyjbi8_cnxhnba;/data1/app_comment239n7zjqyjbi8_entertainment;/data1/app_comment239n7zjqyjbi8_finance;/data1/app_comment239n7zjqyjbi8_newscenter;/data1/app_comment239n7zjqyjbi8_sports;/data1/app_comment239n7zjqyjbi8_techc;/data1/app_picserversweibof6vwt_unidownloadlua;/data1/app_picserversweibof6vwt_uniuploadlua;/data1/appsafe_log;/data1/app_sinamailaqpfprgge53u_webface;/data1/app_sinamailaqpfprgge53u_webmail;/data1/app_sinamailaqpfprgge53u_webmailvip;/data1/app_sinatqt7c88c2f7f9lht_nginx;/data1/app_sinatqt7c88c2f7f9lht_tqtaction;/data1/app_sinatqt7c88c2f7f9lht_tqtclient;/data1/app_vdisk610m1wfce1jbycv_apilog;/data1/app_vdisk610m1wfce1jbycv_vdiskapiacc;/data1/app_weibo239n7zjqyjbi8_cnxhv25wb;/data1/app_weibomobile03x4ts1kl_appleiap;/data1/app_weibomobile03x4ts1kl_mweibouspeed;/data1/app_weibosubproductwddbx_starvip;/data1/app_weibosubproductwddbx_vipcn;/data1/comment;/data1/err_sinaedgeahsolci14ydn_ppo;/data1/feedad;/data1/playbhwangsu;/data1/sinadb;/data1/vdiskapi;/data1/vdisk_app;/data1/vdiskweb;/data1/wangsu;/data1/waplog;/data1/webmail;/data1/www_dpooltblogeluddtedj2_www;/data1/www_picserversweibof6vwt_unidownloadnginx;/data1/www_picserversweibof6vwt_uniuploadnginx;/data1/www_saetg3to1nis2pe87rbv_guess2014;/data1/www_sinaedgeahsolci14ydn_playbhwangsu;/data1/www_sinaedgeahsolci14ydn_sae;/data1/www_sinaedgeahsolci14ydn_wangsu;/data1/www_sinaedgeahsolci14ydn_wsedgeiask;/data1/www_sinaedgeahsolci14ydn_wsweibogrid;/data1/www_sinaedgeahsolci14ydn_wsweiboimg;/data1/www_spoollxrsaansnq8tjw0_weibojs;/data1/www_wapmobileqdo0pwqdak8_waplog;/data1/www_weibohomecjnp8usojrt_vipcn;/data1/www_weiboplatformmz4oexg_feedad;/data1/www_sinagameilfsnmph7zu1_register;/data1/www_sinagameilfsnmph7zu1_login;/data1/www_sinagameilfsnmph7zu1_platform;/data1/www_sinagameilfsnmph7zu1_pay;/data1/www_sinagameilfsnmph7zu1_gameinfo;/data1/www_sinagameilfsnmph7zu1_gameserver;/data1/www_sinagameilfsnmph7zu1_sports;/data1/www_sinagameilfsnmph7zu1_weibo;/data1/www_sinagameilfsnmph7zu1_libra;/data1/www_sinagameilfsnmph7zu1_wwregister;/data1/www_spoollxrsaansnq8tjw0_kuaiwangXweibo;/data1/cdn/data;/data1/www_sinagameilfsnmph7zu1_channel;/data1/www_sinagameilfsnmph7zu1_channelcatalog;/data1/www_sinagameilfsnmph7zu1_substation;/data1/www_sinagameilfsnmph7zu1_promotionlink;/data1/app_weiboapitdgrb4ef0v32_weibomsg;/data1/ali/data;/data1/app_picserversweibof6vwt_cachel2ha;/data1/app_diptest1234accesskey_dipvisitlog;/data1/app_sinamailaqpfprgge53u_contentlog;/data1/app_picserversweibof6vwt_wapvideodownload;/data1/rsync_hub";

        String[] words = line.split(";");

        for (String word : words) {
            System.out.println(word);
        }
    }

}
