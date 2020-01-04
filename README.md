## AWS Glue: 開発環境

AWS Glueのlocal開発/テスト環境を格納するrepositoryです.

LocalStackを用いた疑似開発環境を構成しています.

### 1. ディレクトリ構成
```
data
src
 L libs
   L function.py
 L etl.py
tests
 L confest.py
 L test_etl.py
docker-compose.yml
Dockerfile
```

### 2. 使い方

1.  Glue開発環境, LocalStackの build

     ``` docker-compose up -d --build ```

2. 開発
   ひな形ファイルに沿って記載
   
   ```src/etl.py```
   : gluejob本体.
   
   ```src/libs/functions.py```
   : ETL処理の関数を一部切り出したモジュール

   ```tests/test_etl.py```
   : etl.pyの関数のテストスクリプト

   ```tests/test_function.py```
   : function.pyの関数のテストスクリプト

3. テスト

    テストは下記のコマンドで実行

    ```docker exec -it gluelocal gluepytest```

    localStack上のジョブの実行は下記のコマンドで実行

    ```docker exec -it gluelocal gluesparksubmit```

### 3. 参考URL
https://dev.classmethod.jp/cloud/aws/aws-glue-local/

https://future-architect.github.io/articles/20191206/

