# AlertSystem

## Steps to run alert system –

#### 1. Download the zip as shown below.
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image1.jpg?raw=true)



#### 2. Extract the zip. Below folder will get created.
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image2.jpg?raw=true) 



#### 3. Take the AlertSystemControl project folder out and place it in c drive or d drive.
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image3.jpg?raw=true)



#### 4. Then launch eclipse – scala and create a sample workspace as below screenshot. Here, the sample workspace is AlertSystem in D drive.
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image4.jpg?raw=true)



#### 5.	Then go to File  Open Projects from file system and browse the path where the project folder has been extracted and click create as shown below.
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image5.jpg?raw=true)



#### 6.	Since this is a maven project, all necessary jars will get downloaded w.r.t. the pom.xml file. Next go to the build path and change the scala library from 2.12.3  to 2.11.11 version as spark is supported on 2.11.11
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image6.jpg?raw=true)
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image7.jpg?raw=true)
 
 
 
#### 7.	Now execute the below programs in the following order:
1.	readSensorData.scala
2.	createStreamSource.scala
3.	imputeData.scala
Below screenshot showing all the three programs running simultaneously to produce the control system with alert configuration.
![alt text](https://github.com/saikatsengupta89/AlertSystem/blob/master/images/Image8.jpg?raw=true)
