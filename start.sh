
docker build --rm -t echarts  -f Dockerfile .
docker run -itd --name=mqEcharts -v /data/PyechartsScript/wwwroot:/app/wwwroot/ echarts
