#!/bin/bash
_phone="18518128264"
_mail="xuhaiyang03@baidu.com"
#后续应该移到配置中
function Alert ()
{
        local fnEMAIL=$3
        local fnMOBILE=$4
        local fnLOG=$2
        local fnMSG=$1

        [[ -z $fnMSG ]] && return 1

        fninfo=`hostname`:$fnMSG\[`date +%T`\]
        if [[ -z $fnEMAIL ]] ; then
                return 1
        else
                echo | mail -s "$fninfo" $fnEMAIL
        fi

        [[ -z $fnMOBILE ]] && return 1
        for fnMBLID in $fnMOBILE
        do
                gsmsend -s emp01.baidu.com:15003 -s emp02.baidu.com:15003 $fnMBLID@"$fninfo"
        done
}

function Warning()
{
    _msg=${1}
    Alert "${_msg}" "${_msg}" "${_mail}" "${_phone}"

}

function Check_Value()
{
   if [ $1 != 0 ] ; then
        _msg=$2
        Alert "${_msg}" "${_msg}" "${_mail}" "${_phone}"
        exit -1
   fi
}

function Warning_Email()
{
    if [ $1 != 0 ] ; then
        _msg=$2
        Alert "${_msg}" "${_msg}" "${_mail}"
    fi
}
