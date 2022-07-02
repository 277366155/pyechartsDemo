FROM fnndsc/ubuntu-python3 
RUN pip install pymssql pyecharts
ENV TZ=Asia/Shanghai
WORKDIR /app
COPY . /app
EXPOSE 80
ENTRYPOINT ["python3","exportCharts.py"]
