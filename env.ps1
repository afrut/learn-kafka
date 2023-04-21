$arrPath = @(
"C:\WINDOWS\system32",
"C:\WINDOWS\system32\wbem",
"C:\Program Files\PowerShell\7",
"D:\Python\Python310\Scripts\",
"D:\Python\Python310\",
"D:\kafka_2.13-3.1.0\bin\windows",
"C:\Program Files\Java\jdk1.8.0_202\bin",
"D:\Program Files\Git\cmd",
"D:\scripts"
)
$Env:PATH = $arrPath -join ";"
$Env:JAVA_HOME="C:\Program Files\Java\jdk1.8.0_202"
$Env:KAFKA_HOME="/mnt/d/kafka_2.13-3.1.0"