setlocal
cd /d %~dp0
@echo off
cmd /k "powershell -ExecutionPolicy Bypass -File update_chromedriver.ps1 & exit"
