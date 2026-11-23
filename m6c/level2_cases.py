#!/usr/bin/env python3
# 第三批交接文档第四节：二级触发条件复核的原始事实（第二批重跑运行 a 的 D、A 两例）。
# 列出每例一级切换前后到二级决定之间该台的性能层事件（非候选），以及数据层日记录中一级触发通道或当日偏离的通道；
# 另列一级决定日之前几天的触发通道偏移，用于对照偏移方向。只报事实，不下结论。
#
# 执行环境：开发机，仓库根目录，Python 3.8+（只用标准库）。
# 调用命令：python3 m6c/level2_cases.py > docs/m6c/level2_cases.txt
# 前置条件：docs/m6b/full/a/ 下有 drift.jsonl（989 行）、m5a_records.jsonl（2,140 行）与 adapt.jsonl。
# 期望产出：标准输出逐例打印切换时刻、性能层事件（当地时间）与数据层日记录。
# 失败兜底：缺文件时 Python 报 FileNotFoundError；第一次全量重放的 B 例没有这两份文件，不在本脚本内。
import json,datetime
u=lambda t: datetime.datetime.utcfromtimestamp(t).strftime('%m-%d %H:%M')
def loc(t):
    off=3600 if t>=1648342800 else 0
    return datetime.datetime.utcfromtimestamp(t+off).strftime('%m-%d %H:%M')
D=[json.loads(l) for l in open('docs/m6b/full/a/drift.jsonl')]
R=[json.loads(l) for l in open('docs/m6b/full/a/m5a_records.jsonl') if '"m5aDay"' in l]
C={}
for l in open('docs/m6b/full/a/adapt.jsonl'):
    c=json.loads(l)
    if c.get('type')=='command': C[(c['device'],c['seq'])]=c
for dev,s1,s2 in (('D',3,4),('A',5,6)):
    c1,c2=C[(dev,s1)],C[(dev,s2)]
    sw=c1['effectiveAt']+3*86400
    print('=== %s L1 seq %d triggers %s, switch %s UTC (%s local); L2 decided %s UTC'%(dev,s1,c1['triggers'],u(sw),loc(sw),u(c2['decidedAt'])))
    for e in D:
        if e['device']==dev and e['level']!='CANDIDATE' and sw-86400<=e['ts']<=c2['decidedAt']:
            print('  perf %-7s ts %s local, candidate %s local, magnitude %.3f'%(e['level'],loc(e['ts']),loc(e['candidateTs']),e['magnitude']))
    day0=datetime.datetime.utcfromtimestamp(sw-3*86400).strftime('%Y-%m-%d'); day1=datetime.datetime.utcfromtimestamp(c2['decidedAt']).strftime('%Y-%m-%d')
    for r in R:
        if r['device']==dev and day0<=r['m5aDay']<=day1 and r['m5aChannel'] in ('Temperature','Gas','Light','Humidity','Pressure'):
            if r['m5aChannel'] in c1['triggers'] or r['m5aDeviated']:
                print('  day %s %-11s v%d evaluable %s deviated %s D %.3f P %.2g shift %+.3f %s'%(r['m5aDay'],r['m5aChannel'],r['version'],int(r['m5aEvaluable']),int(r['m5aDeviated']),r['m5aD'],r['m5aP'],r['m5aShift'],r['m5aReason'] or ''))
print('=== 一级决定日之前的触发通道 / trigger channels before the level-1 decision')
for dev,a,b in (('D','2022-03-20','2022-03-25'),('A','2022-04-02','2022-04-06')):
    for r in R:
        if r['device']==dev and a<=r['m5aDay']<=b and r['m5aChannel'] in ('Temperature','Gas'):
            print('  %s day %s %-11s v%d deviated %d D %.3f shift %+.3f'%(dev,r['m5aDay'],r['m5aChannel'],r['version'],int(r['m5aDeviated']),r['m5aD'],r['m5aShift']))
