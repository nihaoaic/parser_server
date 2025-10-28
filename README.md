下载oss文件
C:/Users/26567/anaconda3/envs/big-data/python.exe f:/parser_server/parser_cli.py  oss-download-folder --folder-prefix "zfw/1760441100" --local-dir "./spider_log"

线上解析
& C:/Users/26567/anaconda3/envs/big-data/python.exe parser_cli.py oss-parse zfw --object-key zfw/1760441250/


本地解析线下文件
& C:/Users/26567/anaconda3/envs/big-data/python.exe parser_cli.py parse zfw --log-identifier zfw/1760441130  