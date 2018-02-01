@echo off
plink.exe -v -x -a -T -C -noagent -ssh -L 127.0.0.1:1234:213.251.13.170:22022 root@213.251.13.170