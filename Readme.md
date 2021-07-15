# Readme

This repository contains the code to reproduce the measurements from the section "Querying building data" of the paper "Executing ad-hoc queries on big geospatial data sets without acceleration structures".

To run the tests, follow the steps below
1. Download the Enhanced New York City 3D Building Model from https://github.com/georocket/new-york-city-model-enhanced 
2. Execute this project using gradle:
    ```
    ./gradlew run --args="--test 1 --input /PATH/TO/DA_WISE_GML_enhanced/DA12_3D_Buildings_Merged.gml"
    ```

Change the argument `--test 1` to other numbers to execute the other tests.

Remove the filename from the argument `--input` to search in all files of the directory.

To run the program in benchmark mode, add the parameter `--benchmark`. You have to execute the program with `sudo` because it clears the disk cache between the iterations. 
```
sudo ./gradlew run --args="--test 1 --input /home/hendrik/data/gml/new-york/DA_WISE_GML_enhanced/ --benchmark"
```

The code was tested using Ubuntu and macOS.
