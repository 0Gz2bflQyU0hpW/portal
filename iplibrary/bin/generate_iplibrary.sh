java_home='/usr/java/default'

project_dir='/data0/workspace/iplibrary/iplibrary'

project_res_dir=${project_dir}/res

iplibrary_dir='/data0/dipplat/software/systemfile/iplibrary'

iplibrary_tmp_dir=${iplibrary_dir}/00000000

generate_date=`date +%Y%m%d`

echo "geterate date: ${generate_date}"

expiration_date=`date -d "7 days ago" +%Y%m%d`

echo "expiration date: ${expiration_date}"

for entry in `ls $iplibrary_dir`; do
    if [ "$entry" -lt "$expiration_date" ]; then
        rm -rf $iplibrary_dir/$entry
        echo "$entry expired, delete"
    fi
done

iplibrary_generate_dir=${iplibrary_dir}/${generate_date}

mkdir -p ${project_res_dir}

cd ${project_res_dir}

rm -rf type.xml
rm -rf region.xml
rm -rf isp.xml
rm -rf ipdata.csv

wget http://ipdb.intra.sina.com.cn:8080/normal/type.xml
wget http://ipdb.intra.sina.com.cn:8080/normal/region.xml
wget http://ipdb.intra.sina.com.cn:8080/normal/isp.xml
wget http://ipdb.intra.sina.com.cn:8080/normal/ipdata.full.zip
unzip ipdata.full.zip
rm -rf ipdata.full.zip

rm -rf ${iplibrary_tmp_dir}

$java_home/bin/java -Xmx6144m -cp ${project_dir}/conf/:${project_dir}/res/:${project_dir}/target/iplibrary-2.0.0.jar:${project_dir}/target/iplibrary-2.0.0-lib/* com.weibo.dip.iplibrary.util.GenerateIpLibraryMain ${iplibrary_tmp_dir}

if [ $? -eq 0 ]; then
    echo "generate ip library success"
    mv ${iplibrary_tmp_dir} ${iplibrary_generate_dir}
else
    echo "generate ip library failed"
fi

rm -rf ${iplibrary_tmp_dir}