BASE='/Users/bgreenwood/SMODE/waveglider'

echo "$(date -u +'%Y/%m/%d %H:%M:%S') Downloading waveglider data from WGMS"
#curl -s -k -H "Content-Type: text/xml; charset=utf-8" --dump-header sioairsea.header -H "SOAPAction:" -d @sioairsea.xml -X POST https://gliders.wgms.com/webservices/entityapi.asmx
curl -s -b "$BASE/sioairsea.header" "http://sioairsea.wgms.com/pages/exportPage.aspx?viewid=77372&entitytype=37" > "$BASE/sioairsea.csv"
python2.7 "$BASE/decode.py"
python2.7 "$BASE/plot.py"
