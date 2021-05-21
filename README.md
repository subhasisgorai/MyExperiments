# Introduction 

This space contains some of my experimental works. I have written a few light weight libraries for Data Processing Pipeline or for interacting with Azure Storage service, i.e., Tables or Queues, in reactive fashion. The libraries leverage Java multithreading and reactive frameworks like RxJava extensively. 

# Getting Started
In order to use this Java SDK in your Java based project, you have to add the following maven dependency,
	
	<dependency>
	    <groupId>com.webworkz.playground</groupId>
	    <artifactId>my-experiments</artifactId>
	    <version>x.y.z</version>
	</dependency>

# Build and Test
For building and testing the project,
<ol> 
<li>firstly `git clone` the project,</li>
<li>then `mvn -U clean install` in order to build the project.</li>
<li>Additionally, if you need to build Eclipse workspace using `mvn eclipse:eclipse -DdownloadSources=true  -DdownloadJavadocs=true` this will fetch the required binaries, sources and documentation and generate the Eclipse workspace.</li>
</ol>
<p>Please note that there would be generated sources, if you are using IDE like Eclipse or IntelliJ Idea, you may need to add the generated artifact as source in order to avoid compilation errors.

# Contribute
You are most welcome to contribute to this project, just fork it or create a branch and start developing. Once you are good please create a pull request and notify.