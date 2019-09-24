#!/bash/bin
days=(20190718 20190719 20190720 20190721 20190722 20190723 20190724)
for day in ${days[*]};
do
    bash -x shell/extract_appusage_data_daily.sh $day
done
