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

rsync -arv --delete --bwlimit=10240 rsync.intra.dip.weibo.com::nfs/iplibrary/$generate_date/* $iplibrary_tmp_dir

if [ $? -eq 0 ]; then
    echo "rsync ip library $generate_date success"
    mv ${iplibrary_tmp_dir} ${iplibrary_generate_dir}
else
    echo "rsync ip library $generate_date failed"
fi

rm -rf ${iplibrary_tmp_dir}