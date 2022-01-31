"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[459],{3905:function(e,t,n){n.d(t,{Zo:function(){return p},kt:function(){return m}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},o=Object.keys(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(a=0;a<o.length;a++)n=o[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var s=a.createContext({}),d=function(e){var t=a.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=d(e.components);return a.createElement(s.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},c=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,o=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),c=d(n),m=r,h=c["".concat(s,".").concat(m)]||c[m]||u[m]||o;return n?a.createElement(h,i(i({ref:t},p),{},{components:n})):a.createElement(h,i({ref:t},p))}));function m(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var o=n.length,i=new Array(o);i[0]=c;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:r,i[1]=l;for(var d=2;d<o;d++)i[d]=n[d];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}c.displayName="MDXCreateElement"},3148:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return l},contentTitle:function(){return s},metadata:function(){return d},toc:function(){return p},default:function(){return c}});var a=n(7462),r=n(3366),o=(n(7294),n(3905)),i=["components"],l={id:"setup",title:"Technical Setup"},s=void 0,d={unversionedId:"getting-started/setup",id:"getting-started/setup",title:"Technical Setup",description:"Requirements",source:"@site/docs/getting-started/setup.md",sourceDirName:"getting-started",slug:"/getting-started/setup",permalink:"/docs/getting-started/setup",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/setup.md",tags:[],version:"current",frontMatter:{id:"setup",title:"Technical Setup"},sidebar:"docs",previous:{title:"Features",permalink:"/docs/features"},next:{title:"Inputs",permalink:"/docs/getting-started/get-input-data"}},p=[{value:"Requirements",id:"requirements",children:[],level:2},{value:"Build Spark docker image",id:"build-spark-docker-image",children:[],level:2},{value:"Compile Scala Classes",id:"compile-scala-classes",children:[],level:2},{value:"Run SDL with Spark docker image",id:"run-sdl-with-spark-docker-image",children:[],level:2},{value:"Development Environment",id:"development-environment",children:[{value:"Hadoop Setup (Needed for Windows only)",id:"hadoop-setup-needed-for-windows-only",children:[],level:3},{value:"Run SDL in IntelliJ",id:"run-sdl-in-intellij",children:[],level:3}],level:2}],u={toc:p};function c(e){var t=e.components,n=(0,r.Z)(e,i);return(0,o.kt)("wrapper",(0,a.Z)({},u,n,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"requirements"},"Requirements"),(0,o.kt)("p",null,"To run this tutorial you just need two things:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},(0,o.kt)("a",{parentName:"li",href:"https://www.docker.com/get-started"},"Docker"),", including docker-compose. If you use Windows, please read our note on ",(0,o.kt)("a",{parentName:"li",href:"/docs/getting-started/troubleshooting/docker-on-windows"},"Docker for Windows"),","),(0,o.kt)("li",{parentName:"ul"},"The ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/smart-data-lake/getting-started"},"source code of the example"),".")),(0,o.kt)("h2",{id:"build-spark-docker-image"},"Build Spark docker image"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Download the source code of the example either via git or by ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip"},"downloading the zip")," and extracting it."),(0,o.kt)("li",{parentName:"ul"},"Open up a terminal and change to the folder with the source, you should see a file called Dockerfile. "),(0,o.kt)("li",{parentName:"ul"},"Then run (note: this might take some time, but it's only needed once):")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"docker build -t sdl-spark .\n")),(0,o.kt)("h2",{id:"compile-scala-classes"},"Compile Scala Classes"),(0,o.kt)("p",null,"Compile included Scala classes. Note: this might take some time, but it's only needed at the beginning or if Scala code has changed."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'mkdir .mvnrepo\ndocker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n')),(0,o.kt)("h2",{id:"run-sdl-with-spark-docker-image"},"Run SDL with Spark docker image"),(0,o.kt)("p",null,"Now let's see Smart Data Lake in action!"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"mkdir -f data\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download\n")),(0,o.kt)("p",null,"This creates a folder in the current directory named ",(0,o.kt)("em",{parentName:"p"},"data")," and then\nexecutes a simple data pipeline that downloads two files from two different websites into that directory."),(0,o.kt)("p",null,"When the execution is complete, you should see the two files in the ",(0,o.kt)("em",{parentName:"p"},"data")," folder.\nWonder what happened ? You will create the data pipeline that does just this in the first steps of this guide."),(0,o.kt)("p",null,"If you wish, you can start with ",(0,o.kt)("a",{parentName:"p",href:"get-input-data"},"part 1")," right away.\nFor parts 2 and later, it is recommended to setup a Development Environment."),(0,o.kt)("h2",{id:"development-environment"},"Development Environment"),(0,o.kt)("p",null,"For some parts of this tutorial it is beneficial to have a working development environment ready. In the following we will mainly explain how one can configure a working evironment for\nWindows or Linux. We will focus on the community version of Intellij. Please ",(0,o.kt)("a",{parentName:"p",href:"https://www.jetbrains.com/idea/"},"download")," the version that suits your operating system. "),(0,o.kt)("h3",{id:"hadoop-setup-needed-for-windows-only"},"Hadoop Setup (Needed for Windows only)"),(0,o.kt)("p",null,"Windows Users need to follow the steps below to have a working Hadoop Installation :"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"First download the Windows binaries for Hadoop ",(0,o.kt)("a",{parentName:"li",href:"https://github.com/cdarlint/winutils/archive/refs/heads/master.zip"},"here")),(0,o.kt)("li",{parentName:"ol"},"Extract the wished version to a folder (e.g. ","<"," prefix ",">","\\hadoop-","<"," version ",">","\\bin ). For this tutorial we use the version 3.2.2."),(0,o.kt)("li",{parentName:"ol"},"Configure the ",(0,o.kt)("em",{parentName:"li"},"HADOOP_HOME")," environment variable to point to the folder ","<"," prefix ",">","\\hadoop-","<"," version ",">","\\"),(0,o.kt)("li",{parentName:"ol"},"Add the ",(0,o.kt)("em",{parentName:"li"},"%HADOOP_HOME%\\bin")," to the ",(0,o.kt)("em",{parentName:"li"},"PATH")," environment variable")),(0,o.kt)("h3",{id:"run-sdl-in-intellij"},"Run SDL in IntelliJ"),(0,o.kt)("p",null,"We will focus on the community version of Intellij. Please ",(0,o.kt)("a",{parentName:"p",href:"https://www.jetbrains.com/idea/"},"download")," the version that suits your operating system.\nThis needs an Intellij and Java SDK installation. Please make sure you have:"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"Java 8 SDK or Java 11 SDK"),(0,o.kt)("li",{parentName:"ul"},"Scala Version 2.12. You need to install the Scala-Plugin with this exact version and DO NOT UPGRADE to Scala 3. For the complete list of versions at play in SDLB, ",(0,o.kt)("a",{parentName:"li",href:"../reference/build"},"you can consult the Reference"),".")),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Load the project as a maven project: Right-click on pom.xml file -> add as Maven Project"),(0,o.kt)("li",{parentName:"ol"},"Ensure all correct dependencies are loaded: Right-click on pom.xml file, Maven -> Reload Project"),(0,o.kt)("li",{parentName:"ol"},"Configure and run the following run configuration in IntelliJ IDEA:",(0,o.kt)("ul",{parentName:"li"},(0,o.kt)("li",{parentName:"ul"},"Main class: ",(0,o.kt)("inlineCode",{parentName:"li"},"io.smartdatalake.app.LocalSmartDataLakeBuilder")),(0,o.kt)("li",{parentName:"ul"},"Program arguments: ",(0,o.kt)("inlineCode",{parentName:"li"},"--feed-sel <regex-feedname-selector> --config $ProjectFileDir$/config")),(0,o.kt)("li",{parentName:"ul"},"Working directory: ",(0,o.kt)("inlineCode",{parentName:"li"},"/path/to/sdl-examples/target")," or just ",(0,o.kt)("inlineCode",{parentName:"li"},"target"))))),(0,o.kt)("p",null,(0,o.kt)("strong",{parentName:"p"},"Congratulations!")," You're now all setup! Head over to the next step to analyse these files..."))}c.isMDXComponent=!0}}]);