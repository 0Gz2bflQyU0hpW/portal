yesterdayHdfs=`date --date='1 days ago' +%Y_%m_%d`
yesterdayDes=`date --date='1 days ago' +%Y-%m-%d`
hdfsPath=/tmp/youku/$yesterdayHdfs/*
echo $(date): hdfsPath=$hdfsPath

outputPath1=10.73.89.113::video/$yesterdayDes/
outputPath2=10.73.89.114::video/$yesterdayDes/

tmpPath="/data0/xiaoyu/tmp/youku/"
if [ ! -x "$tmpPath" ]; then
mkdir "$tmpPath"
fi

echo $(date): rm -rf $tmpPath*
rm -rf $tmpPath*

echo $(date): outputPath1=$outputPath1
echo $(date): outputPath2=$outputPath2

echo $(date): hadoop --config /etc/hadoop/conf-online/ fs -copyToLocal $hdfsPath $tmpPath

hadoop --config /etc/hadoop/conf-online/ fs -copyToLocal $hdfsPath $tmpPath
if [ "$?" = 1 ]; then
echo $(date): "copyToLocal failed.stop."
exit 1
fi

echo $(date): rsync -arv $tmpPath $outputPath1
rsync -arv $tmpPath $outputPath1

echo $(date): rsync -arv $tmpPath $outputPath2
rsync -arv $tmpPath $outputPath2

echo -e "\n" >> /data0/xiaoyu/operation.txt

#rm -rf "$tmpPath*" 
