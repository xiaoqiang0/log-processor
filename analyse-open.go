package main

import (
    "bufio"
    "encoding/json"
    "flag"
    "fmt"
    "github.com/mediocregopher/radix.v2/pool"
    str "github.com/mgutz/str"
    dataurl "github.com/vincent-petithory/dataurl"
    "github.com/wangtuanjie/ip17mon"
    "io"
    "os"
    "strconv"
    "strings"
    "sync"
    "time"
)

const DIGLOGFILE string = "/data/var/log/digserver.log"
const REALTIMEDIGLOGFILE string = "/data/var/log/digserver-realtime.log"

type diglog struct {
    Table     string `json:"table"`
    Operation string `json:"operation"`
    Rowkey    string `json:"rowkey"`
    Family    string `json:"family"`
    Qualifier string `json:"qualifier"`
    Value     int    `json:"value"`
    Time      string `json:"time"`
}

var ipFindMutex sync.Mutex

/**
 * record PV
 */
var recordMutex sync.Mutex
var recordData = make(map[string]map[string]map[string]map[string]int) // eg. recordData["lianjaiweb"]["20160321_xxxxx_xxx_xxx"]["pv"]["total"]
var recordLastTimeMutex sync.Mutex
var recordLastTime = time.Now().Unix()
var recordGap = int64(300)

/**
 * pageTop data
 */
var pageTopMutex sync.Mutex
var pageTopData = make(map[string]map[string]map[string]int) // eg. pageTopData["lianjaiweb"]["20160321_xxxxx_xxx_xxx"]["http://xxxxx"]
var pageTopLastTimeMutex sync.Mutex
var pageTopLastTime = time.Now().Unix()
var pageTopGap = int64(600)

/**
 * Jumpto data
 */
var jumptoMutex sync.Mutex
var jumptoData = make(map[string]map[string]map[string]int) // eg. jumptoData["lianjaiweb"]["20160321_xxxxx_xxx_xxx"]["20160321_xxxxx_xxx_xxx"]
var jumptoLastTimeMutex sync.Mutex
var jumptoLastTime = time.Now().Unix()
var jumptoGap = int64(600)

/**
 * UV data
 */
var uvStackMutex sync.Mutex
var uvStackData = make(map[string]map[string]map[string]map[string]map[string]int) // eg. uvStackData["lianjiaweb"]["20160321_xxxxx_xxx_xxx"]["uv"]["total"]["189397471641241414"]
var uvStackLastTimeMutex sync.Mutex
var uvStackLastTime = time.Now().Unix()
var uvStackGap = int64(600)

/*
 * real time record PV
 */
var realTimeRecordMutex sync.Mutex
var realTimeRecordData = make(map[string]map[string]map[string]map[string]int) // eg. realTimeRecordData["total"]["20160321_xxxxx_xxx_xxx"]["pv"]["total"]
var realTimeRecordLastTimeMutex sync.Mutex
var realTimeRecordLastTime = time.Now().Unix()
var realTimeRecordGap = int64(15)
/**
** 数据块
**/
var repeatStr = " "
var repeatStrLen = 1
var packetLen = 1024 * 4

var cityShortName = map[string]int{
    "bj":  110000,
    "sh":  310000,
}
var cityShortNameGlobal = map[string]int{
    "us": 10001,
    "ca": 10002,
}
var domain2pid = map[string]string{
    "dianpu.lianjia.com": "dianpu",
    "user.lianjia.com":   "user",
}
var columnInfo = map[string]map[string][]string{
    "lianjiaweb": {
        "ershoufang":  []string{"list", "detail", "search"},
    },
    "dianpu": {
        "dianpu": []string{},
    },
}
var subColumnMatchRule = map[string]map[string]string{
    "detail":       {"key": "detail", "match": ".html"},
    "detailh5":     {"key": "detailh5", "match": ".h5"},
}
var monthExchange = map[string]string{
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}

func consumer(ch <-chan string, index int, redisPool *pool.Pool) {
    for log := range ch {
        log = strings.TrimSpace(log)
        pos1 := str.IndexOf(log, "GET /t.gif?", 0)
        if pos1 == -1 {
            continue
        }
        pos1 += 29
        pos2 := str.IndexOf(log, " HTTP/", pos1)
        d := str.Substr(log, pos1, pos2-pos1)
        dstr, err := dataurl.UnescapeToString(d)
        if err != nil {
            continue
        }
        data := make(map[string]interface{})
        dstrByte := []byte(dstr)
        err = json.Unmarshal(dstrByte, &data)
        if err != nil {
            continue
        }
        pid, pidExists := data["pid"]
        key, keyExists := data["key"]
        evt, evtExists := data["evt"]
        if !pidExists || !keyExists || !evtExists || key == nil || pid == nil || evt == nil {
            continue
        }
        /**
         * 临时问题FIX
         */
        var isContinue bool = false
        switch pid.(type) {
        case float64:
            isContinue = true
        }
        if isContinue {
            continue
        }
        /**
         * fix done
         */
        date, dateExists := data["date"]
        if !dateExists {
            date = getUnixTime(log)
        }
        channel, channelExists := data["c"]
        version, versionExists := data["v"]
        if !channelExists {
            channel = "0"
        }
        if !versionExists {
            version = "0"
        }
        uuid, uuidExists := data["uuid"]
        if !uuidExists {
            uid, uidExists := data["uid"]
            if !uidExists {
                uuid = "0"
            } else {
                uuid = uid
            }
        }

        logArray := strings.Split(log, "\" \"")
        logArrayLength := len(logArray)
        ipAddr := strings.Trim(logArray[logArrayLength-1], "\"")
        cityStr := ipFind(ipAddr)
        uaStr := strings.Trim(logArray[logArrayLength-2], "\"")
        deviceStr := deviceFind(uaStr)

        smartKeyData := findSmartKey(key.(string), pid.(string))
        smartKeys := joinSmartKeys("Key", smartKeyData, date.(string), channel.(string), version.(string))

        evts := strings.Split(evt.(string), ",")
        for _, event := range evts {
            if strings.EqualFold(event, "1") {
                // total pv/uv
                realTimeRecord("total", smartKeys, "pv", "total", "", redisPool)
        record("total", smartKeys, "pv", "city_"+cityStr, "", redisPool)
        record("total", smartKeys, "uv", "total", uuid.(string), redisPool)
                // PV
                record(pid.(string), smartKeys, "pv", "total", "", redisPool)
                record(pid.(string), smartKeys, "pv", "city_"+cityStr, "", redisPool)
                record(pid.(string), smartKeys, "pv", deviceStr, "", redisPool)
                // UV
                record(pid.(string), smartKeys, "uv", "total", uuid.(string), redisPool)
                record(pid.(string), smartKeys, "uv", "city_"+cityStr, uuid.(string), redisPool)
                record(pid.(string), smartKeys, "uv", deviceStr, uuid.(string), redisPool)

                // pageTop
                record(pid.(string), smartKeys, "pagetop", key.(string), "", redisPool)
            } else if strings.EqualFold(event, "3") {
                // jumpto
                from, fromExists := data["f"]
                if !fromExists {
                    continue
                }
                fromSmartKeyData := findSmartKey(from.(string), "")
                if fromSmartKeyData["pid"] == nil || fromSmartKeyData["pid"] == "" {
                    continue
                }
                fromSmartKeys := joinSmartKeysForJumpto(fromSmartKeyData, date.(string), channel.(string), version.(string))
                jumpto := jumptoSmartKey(pid.(string), smartKeyData, date.(string), channel.(string), version.(string))
                if jumpto != "" {
                    record(fromSmartKeyData["pid"].(string), fromSmartKeys, "jumpto", jumpto, "", redisPool)
                }
            } else if strings.EqualFold(event, "2") {
                stayTime, stayTimeExists := data["stt"]
                if !stayTimeExists {
                    continue
                }
                stayTimeStr := getStayTimeStr(strconv.Itoa(int(stayTime.(float64))))
                record(pid.(string), smartKeys, "stay", stayTimeStr, "", redisPool)
            } else if len(event) != 0 {
                record(pid.(string), smartKeys, "event", event, "", redisPool)
            }
        }
    }
}

func recordLog(log string) {
    t := time.Now().Format("2006-01-02 15:04:05")
    fmt.Println(t + " " + log + "\n")
}

func realTimeRecord(pid string, smartKeys []string, family string, qualifier string, stackValue string, redisPool *pool.Pool) {
    if strings.EqualFold(family, "pv") {
        realTimeRecordMutex.Lock()
        defer realTimeRecordMutex.Unlock()

        if _, ok := realTimeRecordData[pid]; !ok {
            realTimeRecordData[pid] = make(map[string]map[string]map[string]int)
        }
        for _, smartKey := range smartKeys {
            if _, ok := realTimeRecordData[pid][smartKey]; !ok {
                realTimeRecordData[pid][smartKey] = make(map[string]map[string]int)
            }
            if _, ok := realTimeRecordData[pid][smartKey][family]; !ok {
                realTimeRecordData[pid][smartKey][family] = make(map[string]int)
            }
            realTimeRecordData[pid][smartKey][family][qualifier]++
        }
        if time.Now().Unix()-realTimeRecordLastTime > realTimeRecordGap {
            realTimeRecordLastTimeMutex.Lock()
            defer realTimeRecordLastTimeMutex.Unlock()
            recordLog("Flush real time record start")
            flushRealTimeRecord(redisPool)
            recordLog("Flush real time record over")
            realTimeRecordLastTime = time.Now().Unix()
        }
    }
}
// 累加计数
// var recordData = make(map[string]map[string]map[string]map[string]int)
//eg. recordData["lianjaiweb"]["20160321_xxxxx_xxx_xxx"]["pv"]["total"]
func record(pid string, smartKeys []string, family string, qualifier string, stackValue string, redisPool *pool.Pool) {
    if family == "uv" && (stackValue != "" || stackValue == "0") {
        uvStackMutex.Lock()
        defer uvStackMutex.Unlock()

        if _, ok := uvStackData[pid]; !ok {
            uvStackData[pid] = make(map[string]map[string]map[string]map[string]int)
        }
        for _, smartKey := range smartKeys {
            if _, ok := uvStackData[pid][smartKey]; !ok {
                uvStackData[pid][smartKey] = make(map[string]map[string]map[string]int)
            }
            if _, ok := uvStackData[pid][smartKey][family]; !ok {
                uvStackData[pid][smartKey][family] = make(map[string]map[string]int)
            }
            if _, ok := uvStackData[pid][smartKey][family][qualifier]; !ok {
                uvStackData[pid][smartKey][family][qualifier] = make(map[string]int)
            }
            if _, ok := uvStackData[pid][smartKey][family][qualifier][stackValue]; !ok {
                uvStackData[pid][smartKey][family][qualifier][stackValue] = 1
            }
        }
        if time.Now().Unix()-uvStackLastTime > uvStackGap {
            uvStackLastTimeMutex.Lock()
            defer uvStackLastTimeMutex.Unlock()
            recordLog("Flush UV Data start")
            flushUvStack(redisPool)
            recordLog("Flush UV Data over")
            uvStackLastTime = time.Now().Unix()
        }
    } else if family == "pagetop" {
        pageTopMutex.Lock()
        defer pageTopMutex.Unlock()

        if _, ok := pageTopData[pid]; !ok {
            pageTopData[pid] = make(map[string]map[string]int)
        }
        for _, smartKey := range smartKeys {
            if _, ok := pageTopData[pid][smartKey]; !ok {
                pageTopData[pid][smartKey] = make(map[string]int)
            }
            pageTopData[pid][smartKey][qualifier]++
        }
        if time.Now().Unix()-pageTopLastTime > pageTopGap {
            pageTopLastTimeMutex.Lock()
            defer pageTopLastTimeMutex.Unlock()
            recordLog("Flush PageTop Data start")
            flushPageTop(redisPool)
            recordLog("Flush PageTop Data over")
            pageTopLastTime = time.Now().Unix()
        }
    } else if family == "jumpto" {
        jumptoMutex.Lock()
        defer jumptoMutex.Unlock()

        if _, ok := jumptoData[pid]; !ok {
            jumptoData[pid] = make(map[string]map[string]int)
        }
        for _, smartKey := range smartKeys {
            if _, ok := jumptoData[pid][smartKey]; !ok {
                jumptoData[pid][smartKey] = make(map[string]int)
            }
            jumptoData[pid][smartKey][qualifier]++
        }
        if time.Now().Unix()-jumptoLastTime > jumptoGap {
            jumptoLastTimeMutex.Lock()
            defer jumptoLastTimeMutex.Unlock()
            recordLog("Flush Jumpto Data start")
            flushJumpto(redisPool)
            recordLog("Flush Jumpto Data over")
            jumptoLastTime = time.Now().Unix()
        }
    } else if family != "uv" {
        recordMutex.Lock()
        defer recordMutex.Unlock()

        if _, ok := recordData[pid]; !ok {
            recordData[pid] = make(map[string]map[string]map[string]int)
        }
        for _, smartKey := range smartKeys {
            if _, ok := recordData[pid][smartKey]; !ok {
                recordData[pid][smartKey] = make(map[string]map[string]int)
            }
            if _, ok := recordData[pid][smartKey][family]; !ok {
                recordData[pid][smartKey][family] = make(map[string]int)
            }
            recordData[pid][smartKey][family][qualifier]++
        }
        if time.Now().Unix()-recordLastTime > recordGap {
            recordLastTimeMutex.Lock()
            defer recordLastTimeMutex.Unlock()
            recordLog("Flush record start")
            flushRecord(redisPool)
            recordLog("Flush record over")
            recordLastTime = time.Now().Unix()
        }
    }
}

/**
func flushUvStack(redisPool *pool.Pool) {
     f, err := os.OpenFile(DIGLOGFILE, os.O_APPEND|os.O_WRONLY, 0666)
     if err != nil {
         panic(err)
     }
     w := bufio.NewWriter(f)
         strLen := 0
     for pid, map1 := range uvStackData {
         for rowkey, map2 := range map1 {
             for family, map3 := range map2 {
                 for qualifier, uuids := range map3 {
                     if len(uuids) > 0 {
                         uvCount := 0
                         for uuid, _ := range uuids {
                             if !strings.EqualFold(uuid, "") {
                                 uvCount++
                             }
                         }
                         d := diglog{Table: "tj:" + pid, Operation: "increase", Rowkey: rowkey, Family: family, Qualifier: qualifier, Value: uvCount}
                         jsonData, _ := json.Marshal(d)
                                                 //补全数据块 4*1024字节
                                                 strLen = dataBlockCompletion(strLen, len(jsonData))
                         _, err := w.WriteString(string(jsonData) + "\n")
                         if err != nil {
                             recordLog("FlushRecord error")
                             panic(err)
                         }
                         uvStackData[pid][rowkey][family][qualifier] = make(map[string]int)
                     }
                 }
             }
         }
     }
         dataBlockFlush(strLen)
     w.Flush()
}
**/
func flushUvStack(redisPool *pool.Pool) {
    redis, err := redisPool.Get()
    if err != nil {
        panic(err)
    }
    for pid, map1 := range uvStackData {
        for rowkey, map2 := range map1 {
            for family, map3 := range map2 {
                for qualifier, uuids := range map3 {
                    if len(uuids) > 0 {
                        name := "TJ_" + pid + "_uv_" + rowkey + "_" + qualifier
                        for uuid, _ := range uuids {
                            redis.Cmd("SADD", name, uuid)
                        }
                        uvStackData[pid][rowkey][family][qualifier] = make(map[string]int)
                    }
                }
            }
        }
    }
}
func dataBlockCompletion(strLen int, rowLen int, w *bufio.Writer) int {
    formatLen := rowLen + repeatStrLen
    if (strLen + formatLen) > packetLen {
        subStrLen := (packetLen - strLen - repeatStrLen)
        w.WriteString(strings.Repeat(repeatStr, subStrLen) + "\n")
        strLen = formatLen
    } else if (strLen + formatLen) == packetLen {
        strLen = 0
    } else {
        strLen += formatLen
    }
    return strLen
}
func dataBlockFlush(strLen int, w *bufio.Writer) {
    lastStrLen := (packetLen - strLen - repeatStrLen)
    w.WriteString(strings.Repeat(repeatStr, lastStrLen) + "\n")
}
func flushRealTimeRecord(redisPool *pool.Pool) {
    f, err := os.OpenFile(REALTIMEDIGLOGFILE, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    w := bufio.NewWriter(f)
    strLen := 0
    for pid, map1 := range realTimeRecordData {
        for rowkey, map2 := range map1 {
            for family, map3 := range map2 {
                for qualifier, value := range map3 {
                    if value > 0 {
                        d := diglog{Table: "tj:" + pid, Operation: "increase", Rowkey: rowkey, Family: family, Qualifier: qualifier, Value: value, Time: time.Now().Format("2006-01-02 15:04:05")}
                        jsonData, _ := json.Marshal(d)
                        //补全数据块 4*1024字节
                        strLen = dataBlockCompletion(strLen, len(jsonData), w)
                        _, err := w.WriteString(string(jsonData) + "\n")
                        if err != nil {
                            recordLog("FlushRealTimeRecord error")
                            panic(err)
                        }
                        realTimeRecordData[pid][rowkey][family][qualifier] = 0
                    }
                }
            }
        }
    }
    dataBlockFlush(strLen, w)
    w.Flush()
}
func flushRecord(redisPool *pool.Pool) {
    f, err := os.OpenFile(DIGLOGFILE, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    w := bufio.NewWriter(f)
    strLen := 0
    for pid, map1 := range recordData {
        for rowkey, map2 := range map1 {
            for family, map3 := range map2 {
                for qualifier, value := range map3 {
                    if value > 0 {
                        d := diglog{Table: "tj:" + pid, Operation: "increase", Rowkey: rowkey, Family: family, Qualifier: qualifier, Value: value, Time: time.Now().Format("2006-01-02 15:04:05")}
                        jsonData, _ := json.Marshal(d)
                        //补全数据块 4*1024字节
                        strLen = dataBlockCompletion(strLen, len(jsonData), w)
                        _, err := w.WriteString(string(jsonData) + "\n")
                        if err != nil {
                            recordLog("FlushRecord error")
                            panic(err)
                        }
                        recordData[pid][rowkey][family][qualifier] = 0
                    }
                }
            }
        }
    }
    dataBlockFlush(strLen, w)
    w.Flush()
}

/*
func flushRecord(redisPool *pool.Pool) {
    f, err := os.OpenFile(DIGLOGFILE, os.O_APPEND|os.O_WRONLY, 0666)
    if err != nil {
        panic(err)
    }
    w := bufio.NewWriter(f)
    for pid, map1 := range recordData {
        for rowkey, map2 := range map1 {
            for family, map3 := range map2 {
                for qualifier, value := range map3 {
                    if value > 0 {
                        d := diglog{Table: "tj:" + pid, Operation: "increase", Rowkey: rowkey, Family: family, Qualifier: qualifier, Value: value}
                        jsonData, _ := json.Marshal(d)
                        _, err := w.WriteString(string(jsonData) + "\n")
                        if err != nil {
                            recordLog("FlushRecord error")
                            panic(err)
                        }
                        recordData[pid][rowkey][family][qualifier] = 0
                    }
                }
            }
        }
    }
    w.Flush()
}*/
func flushPageTop(redisPool *pool.Pool) {
    redis, err := redisPool.Get()
    if err != nil {
        panic(err)
    }
    for pid, map1 := range pageTopData {
        for rowkey, map2 := range map1 {
            for qualifier, value := range map2 {
                if value > 0 {
                    name := "TJ_" + pid + "_pagetop_" + rowkey
                    redis.Cmd("ZINCRBY", name, value, qualifier)
                    pageTopData[pid][rowkey][qualifier] = 0
                }
            }
        }
    }
}
func flushJumpto(redisPool *pool.Pool) {
    redis, err := redisPool.Get()
    if err != nil {
        return
    }
    for pid, map1 := range jumptoData {
        for rowkey, map2 := range map1 {
            for qualifier, value := range map2 {
                if value > 0 {
                    name := "TJ_" + pid + "_jumpto_" + rowkey
                    redis.Cmd("ZINCRBY", name, value, qualifier)
                    jumptoData[pid][rowkey][qualifier] = 0
                }
            }
        }
    }
}

// 查询手机设备
func deviceFind(ua string) string {
    if str.IndexOf(ua, "Android", 0) != -1 {
        return "device_android"
    } else if str.IndexOf(ua, "iPhone", 0) != -1 {
        return "device_ios"
    } else {
        return "device_PC"
    }
}

// 互斥查询IP数据
func ipFind(ipStr string) string {
    ipFindMutex.Lock()
    defer ipFindMutex.Unlock()
    loc, err := ip17mon.Find(ipStr)
    if err != nil {
        return ""
    }
    if loc.Country != "中国" {
        return "海外-" + loc.Country
    }
    var r string = ""
    if loc.Region != "" && loc.Region != "N/A" {
        r += loc.Region
    }
    if loc.City != "" && loc.City != loc.Region && loc.City != "N/A" {
        r += "-" + loc.City
    }
    return r
}

// 通过日志内的信息获取日志时间戳
func getUnixTime(key string) string {
    p1 := str.IndexOf(key, "[", 0) + 1
    timeStr := str.Substr(key, p1, str.IndexOf(key, "]", 0)-p1)
    day := str.Substr(timeStr, 0, 2)
    Month := str.Substr(timeStr, 3, 3)
    year := str.Substr(timeStr, 7, 4)
    month := monthExchange[Month]
    return year + month + day
}

// 停留时间秒数，转换为时间区间
func getStayTimeStr(stt string) string {
    stayTime, ok := strconv.Atoi(stt)
    if ok != nil {
        return ""
    }
    var storeKey string
    if stayTime < 4 {
        storeKey = "0-3s"
    } else if stayTime >= 4 && stayTime < 10 {
        storeKey = "4-10s"
    }
    return storeKey
}
func jumptoSmartKey(pid string, group map[string]interface{}, date string, channel string, version string) string {
    var smartKey = ""
    // Tag 的smartKey不进行累加的扩散
    if group["column"] == nil {
        smartKey = date + "_" + channel + "_" + version + "_" + group["city"].(string) + "_0_Key_0"
    } else if group["column"].(string) != "" {
        smartKey = date + "_" + channel + "_" + version + "_" + group["city"].(string) + "_0_Key_" + group["column"].(string)
    }
    if group["subColumn"] != nil && group["subColumn"].(string) != "" {
        smartKey += "-" + group["subColumn"].(string)
    }
    if smartKey == "" {
        return ""
    }
    return pid + "_" + smartKey
}
func joinSmartKeys(keyTag string, group map[string]interface{}, date string, channel string, version string) []string {
    if strings.EqualFold(keyTag, "Tag") {
        var smartKey = ""
        // Tag 的smartKey不进行累加的扩散
        if group["column"] == nil {
            smartKey = date + "_" + channel + "_" + version + "_" + group["city"].(string) + "_0_Tag_0"
        } else {
            smartKey = date + "_" + channel + "_" + version + "_" + group["city"].(string) + "_0_Tag_" + group["column"].(string)
        }
        if group["subColumn"] != nil && group["subColumn"].(string) != "" {
            smartKey += "-" + group["subColumn"].(string)
        }
        return []string{smartKey}
    }
    var smartKeysPrefix []string
    smartKeysPrefix = []string{
        date + "_0_0_0_0",                                                            //渠道：全部---版本：全部---城市站：全部---预留字段
        date + "_" + channel + "_0_0_0",                                              //【渠道】---版本：全部---城市站：全部---预留字段
        date + "_" + channel + "_" + version + "_0_0",                                //【渠道】---【版本】---城市站：全部---预留字段
        date + "_" + channel + "_" + version + "_" + (group["city"].(string)) + "_0", //【渠道】---【版本】---【城市站】---预留字段
        date + "_0_" + version + "_0_0",
        date + "_0_" + version + "_" + (group["city"].(string)) + "_0",
        date + "_0_0_" + (group["city"].(string)) + "_0",
    }
    var smartKeysSuffix []string
    smartKeysSuffix = append(smartKeysSuffix, "0_0")
    if group["column"] != nil && !strings.EqualFold(group["column"].(string), "") {
        smartKeysSuffix = append(smartKeysSuffix, "Key_"+group["column"].(string))
        if group["subColumn"] != nil && !strings.EqualFold(group["subColumn"].(string), "") {
            smartKeysSuffix = append(smartKeysSuffix, "Key_"+group["column"].(string)+"-"+group["subColumn"].(string))
        }
    }

    tmp := map[string]int{}
    var smartKeys []string
    for _, prefix := range smartKeysPrefix {
        for _, suffix := range smartKeysSuffix {
            row := prefix + "_" + suffix
            if _, ok := tmp[row]; !ok {
                smartKeys = append(smartKeys, row)
                tmp[row] = 1
            }
        }
    }

    return smartKeys

}

// 通过相关信息生成smartKeys
func joinSmartKeysForJumpto(group map[string]interface{}, date string, channel string, version string) []string {
    var smartKeysPrefix []string
    smartKeysPrefix = []string{
        date + "_0_0_0_0",
        date + "_" + channel + "_0_0_0",
        date + "_" + channel + "_" + version + "_0_0",
        date + "_" + channel + "_" + version + "_" + (group["city"].(string)) + "_0",
        date + "_0_" + version + "_0_0",
        date + "_0_" + version + "_" + (group["city"].(string)) + "_0",
        date + "_0_0_" + (group["city"].(string)) + "_0",
    }
    var smartKeysSuffix []string
    if group["column"] != nil && !strings.EqualFold(group["column"].(string), "") {
        smartKeysSuffix = append(smartKeysSuffix, "Key_"+group["column"].(string))
        if group["subColumn"] != nil && !strings.EqualFold(group["subColumn"].(string), "") {
            smartKeysSuffix = append(smartKeysSuffix, "Key_"+group["column"].(string)+"-"+group["subColumn"].(string))
        }
    } else if group["column"] != nil && strings.EqualFold(group["column"].(string), "") {
        smartKeysSuffix = append(smartKeysSuffix, "0_0")
    }

    tmp := map[string]int{}
    var smartKeys []string
    for _, prefix := range smartKeysPrefix {
        for _, suffix := range smartKeysSuffix {
            row := prefix + "_" + suffix
            if _, ok := tmp[row]; !ok {
                smartKeys = append(smartKeys, row)
                tmp[row] = 1
            }
        }
    }

    return smartKeys
}

// 通过URL找出SmartKey需要的字段
func findSmartKey(key string, pid string) map[string]interface{} {
    response := make(map[string]interface{})
    response["pid"] = pid
    response["city"] = 0
    response["column"] = ""
    response["subColumn"] = ""

    // 查找PID City信息
    if p := str.IndexOf(key, "://m.lianjia.com", 0); p != -1 {
        // M站
        c := str.Substr(key, p+17, (str.IndexOf(key, "/", p+17) - p - 17))
        if cityId, ok := cityShortName[c]; ok {
            response["city"] = cityId
            response["pid"] = "lianjiamweb"
        }
    } else if p := str.IndexOf(key, "://www.lianjia.com/zhuanti/jia", 0); p != -1 {
        response["city"] = 0
        response["pid"] = "lianjiamweb"
    } else if p := str.IndexOf(key, "://m.sh.lianjia.com", 0); p != -1 {
        // 上海-M站-德佑地产兼容
        response["city"] = 310000
        response["pid"] = "lianjiamweb"
    } else {
        p1 := str.IndexOf(key, "://", 0) + 3
        p := str.Substr(key, p1, str.IndexOf(key, ".", 0)-p1)
        cityId, ok := cityShortName[p]
        if ok {
            response["city"] = cityId
            if str.IndexOf(key, "fang.lianjia.com", 0) != -1 {
                // 新房业务
                response["pid"] = "xinfang"
            } else {
                // 城市站
                response["pid"] = "lianjiaweb"
            }
        } else if _, ok := cityShortNameGlobal[p]; ok {
            // 海外站
            response["city"] = p
            response["pid"] = "overseas"
        } else {
            // 其他无城市维度的站点
            for k, v := range domain2pid {
                if str.IndexOf(key, k, 0) != -1 {
                    response["pid"] = v
                }
            }
        }
    }

    if response["pid"] == nil {
        return response
    }

    // 通过PID信息，找column subColumn
    for k, v := range columnInfo {
        if k != response["pid"] {
            continue
        }
        for matchKey, rules := range v {
            if str.IndexOf(key, matchKey, 0) != -1 {
                response["column"] = matchKey
                hasListRule := false
                for _, rule := range rules {
                    if strings.EqualFold(rule, "list") {
                        hasListRule = true
                    } else {
                        subColumnRule := subColumnMatchRule[rule]
                        if str.IndexOf(key, subColumnRule["match"], 0) != -1 {
                            response["subColumn"] = subColumnRule["key"]
                            break
                        }
                    }
                }
                if strings.EqualFold(response["subColumn"].(string), "") && hasListRule == true {
                    response["subColumn"] = "list"
                } else if strings.EqualFold(response["subColumn"].(string), "") && hasListRule == false && len(rules) == 0 {
                    response["subColumn"] = "index"
                }
                if response["subColumn"] != "" {
                    break
                }
            }
        }
    }

    switch response["city"].(type) {
    case int:
        response["city"] = strconv.Itoa(response["city"].(int))
    }
    return response
}

func ReadLine(filePth string, consumerNumber int, redisPool *pool.Pool) error {
    f, err := os.Open(filePth)
    if err != nil {
        return err
    }
    defer f.Close()

    // Go
    var ch = make(chan string, 40)
    for i := 0; i < consumerNumber; i++ {
        go consumer(ch, i, redisPool)
    }

    r := 0
    bfRd := bufio.NewReader(f)
    for {
        line, err := bfRd.ReadString('\n')
        ch <- line

        r++
        if r%10000 == 0 {
            recordLog("readline:" + strconv.Itoa(r))
        }
        if err != nil {
            if err == io.EOF {
                time.Sleep(5 * time.Second)
                recordLog("wait, readline:" + strconv.Itoa(r))
            } else {
                recordLog("ReadString err")
            }
        }
    }
    return nil
}
func main() {
    flag.Parse()
    if err := ip17mon.Init(flag.Arg(0)); err != nil {
        recordLog("load Ipdata error")
        panic(err)
    }
    file := flag.Arg(1)
    cNumber, _ := strconv.Atoi(flag.Arg(2))

    // redis pool
    redisPool, err := pool.New("tcp", "localhost:6379", cNumber)
    if err != nil {
        recordLog("Connect redis error")
        panic(err)
    }

    ReadLine(file, cNumber, redisPool)
    redisPool.Empty()
    recordLog("File Read Done. redisPool")
}
