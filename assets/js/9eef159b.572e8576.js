"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[29],{3905:function(e,t,n){n.d(t,{Zo:function(){return l},kt:function(){return u}});var a=n(7294);function r(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function i(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function o(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?i(Object(n),!0).forEach((function(t){r(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,r=function(e,t){if(null==e)return{};var n,a,r={},i=Object.keys(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||(r[n]=e[n]);return r}(e,t);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(a=0;a<i.length;a++)n=i[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(r[n]=e[n])}return r}var p=a.createContext({}),c=function(e){var t=a.useContext(p),n=t;return e&&(n="function"==typeof e?e(t):o(o({},t),e)),n},l=function(e){var t=c(e.components);return a.createElement(p.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},m=a.forwardRef((function(e,t){var n=e.components,r=e.mdxType,i=e.originalType,p=e.parentName,l=s(e,["components","mdxType","originalType","parentName"]),m=c(n),u=r,h=m["".concat(p,".").concat(u)]||m[u]||d[u]||i;return n?a.createElement(h,o(o({ref:t},l),{},{components:n})):a.createElement(h,o({ref:t},l))}));function u(e,t){var n=arguments,r=t&&t.mdxType;if("string"==typeof e||r){var i=n.length,o=new Array(i);o[0]=m;var s={};for(var p in t)hasOwnProperty.call(t,p)&&(s[p]=t[p]);s.originalType=e,s.mdxType="string"==typeof e?e:r,o[1]=s;for(var c=2;c<i;c++)o[c]=n[c];return a.createElement.apply(null,o)}return a.createElement.apply(null,n)}m.displayName="MDXCreateElement"},1623:function(e,t,n){n.r(t),n.d(t,{frontMatter:function(){return s},contentTitle:function(){return p},metadata:function(){return c},toc:function(){return l},default:function(){return m}});var a=n(7462),r=n(3366),i=(n(7294),n(3905)),o=["components"],s={id:"custom-webservice",title:"Custom Webservice"},p=void 0,c={unversionedId:"getting-started/part-3/custom-webservice",id:"getting-started/part-3/custom-webservice",title:"Custom Webservice",description:"Goal",source:"@site/docs/getting-started/part-3/custom-webservice.md",sourceDirName:"getting-started/part-3",slug:"/getting-started/part-3/custom-webservice",permalink:"/docs/getting-started/part-3/custom-webservice",tags:[],version:"current",frontMatter:{id:"custom-webservice",title:"Custom Webservice"},sidebar:"docs",previous:{title:"Keeping historical data",permalink:"/docs/getting-started/part-2/historical-data"},next:{title:"Incremental Mode",permalink:"/docs/getting-started/part-3/incremental-mode"}},l=[{value:"Goal",id:"goal",children:[],level:2},{value:"Starting point",id:"starting-point",children:[],level:2},{value:"Define Data Objects",id:"define-data-objects",children:[],level:2},{value:"Define Action",id:"define-action",children:[],level:2},{value:"Try it out",id:"try-it-out",children:[],level:2},{value:"Get Data Frame",id:"get-data-frame",children:[],level:2},{value:"Preserve schema",id:"preserve-schema",children:[],level:2}],d={toc:l};function m(e){var t=e.components,s=(0,r.Z)(e,o);return(0,i.kt)("wrapper",(0,a.Z)({},d,s,{components:t,mdxType:"MDXLayout"}),(0,i.kt)("h2",{id:"goal"},"Goal"),(0,i.kt)("p",null,"  In the previous examples we worked mainly with data that was available as a file or could be fetched with the built-in ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceFileDataObject"),".\nTo fetch data from a webservice, the ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceFileDataObject")," is sometimes not enough and has to be customized.\nThe reasons why the built-in DataObject is not sufficient are manifold, but it's connected to the way Webservices are designed.\nWebservices often include design features like: "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"data pagination"),(0,i.kt)("li",{parentName:"ul"},"protect resources using rate limiting "),(0,i.kt)("li",{parentName:"ul"},"different authentication mechanisms"),(0,i.kt)("li",{parentName:"ul"},"filters for incremental load"),(0,i.kt)("li",{parentName:"ul"},"well defined schema"),(0,i.kt)("li",{parentName:"ul"},"...")),(0,i.kt)("p",null,"Smart Data Lake Builder can not cover all these various needs in a generic ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceDataObject"),", which is why we have to write our own ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject"),".\nThe goal of this part is to learn how such a CustomWebserviceDataObject can be implemented in Scala."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Other than part 1 and 2, we are writing customized Scala classes in part 3 and making use of Apache Spark features.\nAs such, we expect you to have some Scala and Spark know-how to follow along."),(0,i.kt)("p",{parentName:"div"},"It is also a good idea to configure a working development environment at this point.\nIn the ",(0,i.kt)("a",{parentName:"p",href:"/docs/getting-started/setup"},"Technical Setup")," chapter we briefly introduced how to use IntelliJ for development.\nThat should greatly improve your development experience compared to manipulating the file in a simple text editor."))),(0,i.kt)("h2",{id:"starting-point"},"Starting point"),(0,i.kt)("p",null,"Again we start with the ",(0,i.kt)("inlineCode",{parentName:"p"},"application.conf")," that resulted from finishing the last part.\nIf you don't have the application.conf from part 2 anymore, please copy ",(0,i.kt)("a",{target:"_blank",href:n(5471).Z},"this")," configuration file to ",(0,i.kt)("strong",{parentName:"p"},"config/application.conf")," again."),(0,i.kt)("h2",{id:"define-data-objects"},"Define Data Objects"),(0,i.kt)("p",null,"We start by rewriting the existing ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," DataObject.\nIn the configuration file, replace the old configuration with its new definition:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'ext-departures {\n  type = CustomWebserviceDataObject\n  schema = """array< struct< icao24: string, firstSeen: integer, estDepartureAirport: string, lastSeen: integer, estArrivalAirport: string, callsign: string, estDepartureAirportHorizDistance: integer, estDepartureAirportVertDistance: integer, estArrivalAirportHorizDistance: integer, estArrivalAirportVertDistance: integer, departureAirportCandidatesCount: integer, arrivalAirportCandidatesCount: integer >>"""\n  baseUrl = "https://opensky-network.org/api/flights/departure"\n  nRetry = 5\n  queryParameters = [{\n    airport = "LSZB"\n    begin = 1641393602\n    end = 1641483739\n  },{\n    airport = "EDDF"\n    begin = 1641393602\n    end = 1641483739\n  }]\n  timeouts {\n    connectionTimeoutMs = 200000\n    readTimeoutMs = 200000\n  }\n}\n')),(0,i.kt)("p",null,"The Configuration for this new ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," includes the type of the DataObject, the expected schema, the base url from where we can fetch the departures from, the number of retries, a list of query parameters and timeout options.\nTo have more flexibility, we can now configure the query parameters as options instead defining them in the query string.",(0,i.kt)("br",{parentName:"p"}),"\n","The connection timeout corresponds to the time we wait until the connection is established and the read timeout equals the time we wait until the webservice responds after the request has been submitted.\nIf the request cannot be answered in the times configured, we try to automatically resend the request.\nHow many times a failed request will be resend, is controlled by the ",(0,i.kt)("inlineCode",{parentName:"p"},"nRetry")," parameter."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The ",(0,i.kt)("em",{parentName:"p"},"begin")," and ",(0,i.kt)("em",{parentName:"p"},"end")," can now be configured for each airport separatly.\nThe configuration expects unix timestamps, if you don't know what that means, have a look at this ",(0,i.kt)("a",{parentName:"p",href:"https://www.unixtimestamp.com/"},"website"),".\nThe webservice will not respond if the interval is larger than a week.\nHence, we enforce the rule that if the chosen interval is larger, we query only the next four days given the ",(0,i.kt)("em",{parentName:"p"},"begin")," configuration. T"))),(0,i.kt)("p",null,"Note that we changed the type to ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject"),".\nThis is not a standard Smart Data Lake Builder type and only works because we already included the following file for you:  "),(0,i.kt)("ul",null,(0,i.kt)("li",{parentName:"ul"},"./src/scala/io/smartdatalake/workflow/dataobject/CustomWebserviceDataObject.scala")),(0,i.kt)("p",null,"In this part we will work exclusively on the ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject.scala")," file."),(0,i.kt)("h2",{id:"define-action"},"Define Action"),(0,i.kt)("p",null,"In the configuration we only change one action again:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"download-departures {\n  type = CopyAction\n  inputId = ext-departures\n  outputId = stg-departures\n  metadata {\n    feed = compute\n  }\n}\n")),(0,i.kt)("p",null,"The type is no longer ",(0,i.kt)("inlineCode",{parentName:"p"},"FileTransferAction")," but a ",(0,i.kt)("inlineCode",{parentName:"p"},"CopyAction")," instead, as our new ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," converts the Json-Output of the Webservice into a Spark DataFrame."),(0,i.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},(0,i.kt)("inlineCode",{parentName:"p"},"FileTransferAction"),"s are used, when your DataObjects reads an InputStream or writes an OutputStream like ",(0,i.kt)("inlineCode",{parentName:"p"},"WebserviceFileDataObject")," or ",(0,i.kt)("inlineCode",{parentName:"p"},"SFtpFileRefDataObject"),".\nThese transfer files one-to-one from input to output.",(0,i.kt)("br",{parentName:"p"}),"\n","More often you work with one of the many provided ",(0,i.kt)("inlineCode",{parentName:"p"},"SparkAction"),"s like the ",(0,i.kt)("inlineCode",{parentName:"p"},"CopyAction")," shown here.\nThey work by using Spark Data Frames under the hood. "))),(0,i.kt)("h2",{id:"try-it-out"},"Try it out"),(0,i.kt)("p",null,"If this is the first time you're trying out SDL with this version of the tutorial, you need to first build the SDL docker image with the command below.\nNote that this docker image now only includes SDL libraries, and no configuration or code from this project."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"  docker build -t smart-data-lake/gs1 .\n")),(0,i.kt)("p",null,"Then you can compile and execute the code of this project with the following commands.\nNote that parameter ",(0,i.kt)("inlineCode",{parentName:"p"},"--feed-sel")," only selects ",(0,i.kt)("inlineCode",{parentName:"p"},"download-departures")," as Action for execution. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},'  mkdir .mvnrepo\n  docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package\n  docker run --rm -v ${PWD}/target:/mnt/lib -v ${PWD}/data:/mnt/data -v ${PWD}/config:/mnt/config smart-data-lake/gs1:latest --config /mnt/config --feed-sel ids:download-departures\n')),(0,i.kt)("p",null,"Nothing should have changed. You should again receive data as json files in the corresponding ",(0,i.kt)("inlineCode",{parentName:"p"},"stg-departures")," folder.\nBut except of receiving the departures for only one airport, the DataObject returns the departures for all configured airports.\nIn this specific case this would be ",(0,i.kt)("em",{parentName:"p"},"LSZB")," and ",(0,i.kt)("em",{parentName:"p"},"EDDF")," within the corresponding time window."),(0,i.kt)("p",null,"Having a look at the log, something similar should appear on your screen. "),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Prepare started\n2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Prepare succeeded\n2021-11-10 14:00:32 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Init started\n2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979\n2021-11-10 14:00:33 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1630200800&end=1630310979\n2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:237 - Action~download-departures[CopyAction]: Init succeeded\n2021-11-10 14:00:35 INFO  ActionDAGRun$ActionEventListener:228 - Action~download-departures[CopyAction]: Exec started\n2021-11-10 14:00:35 INFO  CopyAction:158 - (Action~download-departures) getting DataFrame for DataObject~ext-departures\n2021-11-10 14:00:36 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1630200800&end=1630310979\n2021-11-10 14:00:37 INFO  CustomWebserviceDataObject:69 - Success for request https://opensky-network.org/api/flights/departure?airport=EDDF&begin=1630200800&end=1630310979\n")),(0,i.kt)("p",null,"It is important to notice that the two requests for each airport to the API were not send only once, but twice.\nThis stems from the fact that the method ",(0,i.kt)("inlineCode",{parentName:"p"},"getDataFrame")," of the Data Object is called twice in the DAG execution of the Smart Data Lake Builder:\nOnce during the Init Phase and once again during the Exec Phase. See ",(0,i.kt)("a",{parentName:"p",href:"/docs/reference/executionPhases"},"this page")," for more information on that.\nBefore we address and mitigate this behaviour in the next section, let's have a look at the ",(0,i.kt)("inlineCode",{parentName:"p"},"getDataFrame")," method and the currently implemented logic:"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'// given the query parameters, generate all requests\nval departureRequests = currentQueryParameters.map(\n  param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"\n)\n// make requests\nval departuresResponses = departureRequests.map(request(_))\n// create dataframe with the correct schema and add created_at column with the current timestamp\nval departuresDf = departuresResponses.toDF("responseBinary")\n  .withColumn("responseString", byte2String($"responseBinary"))\n  .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n  .select(explode($"response").as("record"))\n  .select("record.*")\n  .withColumn("created_at", current_timestamp())\n// return\ndeparturesDf\n')),(0,i.kt)("p",null,"Given the configured query parameters, the requests are first prepared using the request method.\nIf you have a look at the implementation of the ",(0,i.kt)("inlineCode",{parentName:"p"},"request")," method, you notice that we provide some ScalaJCustomWebserviceClient that is based on the ",(0,i.kt)("em",{parentName:"p"},"ScalaJ")," library.\nAlso in the ",(0,i.kt)("inlineCode",{parentName:"p"},"request")," method you can find the configuration for the number of retries.\nAfterwards, we create a data frame out of the response.\nWe implemented some transformations to flatten the result returned by the API.",(0,i.kt)("br",{parentName:"p"}),"\n","Spark has lots of ",(0,i.kt)("em",{parentName:"p"},"Functions")," that can be used out of the box.\nWe used such a column based function ",(0,i.kt)("em",{parentName:"p"},"from_json")," to parse the response string with the right schema.\nAt the end we return the freshly created data frame ",(0,i.kt)("inlineCode",{parentName:"p"},"departuresDf"),"."),(0,i.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"The return type of the response is ",(0,i.kt)("inlineCode",{parentName:"p"},"Array[Byte]"),". To convert that to ",(0,i.kt)("inlineCode",{parentName:"p"},"Array[String]")," a ",(0,i.kt)("em",{parentName:"p"},"User Defined Function")," (also called ",(0,i.kt)("em",{parentName:"p"},"UDF"),") ",(0,i.kt)("inlineCode",{parentName:"p"},"byte2String")," has been used, which is declared inside the getDataFrame method.\nThis function is a nice example of how to write your own ",(0,i.kt)("em",{parentName:"p"},"UDF"),"."))),(0,i.kt)("h2",{id:"get-data-frame"},"Get Data Frame"),(0,i.kt)("p",null,"In this section we will learn how we can avoid sending the request twice to the API using the execution phase information provided by the Smart Data Lake Builder.\nWe will now implement a simple ",(0,i.kt)("em",{parentName:"p"},"if ... else")," statement that allows us to return an empty data frame with the correct schema in the ",(0,i.kt)("strong",{parentName:"p"},"Init")," phase and to only query the data in the ",(0,i.kt)("strong",{parentName:"p"},"Exec")," phase.\nThis logic is implemented in the next code snipped and should replace the code currently enclosed between the two ",(0,i.kt)("inlineCode",{parentName:"p"},"// REPLACE BLOCK")," comments."),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre",className:"language-scala"},'if(context.phase == ExecutionPhase.Init){\n  // simply return an empty data frame\n  Seq[String]().toDF("responseString")\n    .select(from_json($"responseString", DataType.fromDLL(schema)).as("response"))\n    .select(explode($"response").as("record"))\n    .select("record.*")\n    .withColumn("created_at", current_timestamp())\n} else {\n  // use the queryParameters from the config\n  val currentQueryParameters = checkQueryParameters(queryParameters.get)\n\n  // given the query parameters, generate all requests\n  val departureRequests = currentQueryParameters.map(\n    param => s"${baseUrl}?airport=${param.airport}&begin=${param.begin}&end=${param.end}"\n  )\n  // make requests\n  val departuresResponses = departureRequests.map(request(_))\n  // create dataframe with the correct schema and add created_at column with the current timestamp\n  val departuresDf = departuresResponses.toDF("responseBinary")\n    .withColumn("responseString", byte2String($"responseBinary"))\n    .select(from_json($"responseString", DataType.fromDDL(schema)).as("response"))\n    .select(explode($"response").as("record"))\n    .select("record.*")\n    .withColumn("created_at", current_timestamp())\n  \n  // put simple nextState logic below\n  \n  // return\n   departuresDf\n}\n')),(0,i.kt)("p",null,"Don't be confused about some comments in the code. They will be used in the next chapter.\nIf you rebuild the docker image and then restart the program you should see that we do not query the API twice anymore."),(0,i.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,i.kt)("div",{parentName:"div",className:"admonition-heading"},(0,i.kt)("h5",{parentName:"div"},(0,i.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,i.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,i.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,i.kt)("div",{parentName:"div",className:"admonition-content"},(0,i.kt)("p",{parentName:"div"},"Use the information of the ",(0,i.kt)("inlineCode",{parentName:"p"},"ExecutionPhase")," in your custom implementations whenever you need to have different logic during the different phases."))),(0,i.kt)("h2",{id:"preserve-schema"},"Preserve schema"),(0,i.kt)("p",null,"With this implementation, we still write the Spark data frame of our ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," in Json format.\nAs a consequence, we lose the schema definition when the data is read again.",(0,i.kt)("br",{parentName:"p"}),"\n","To improve this behaviour, let's directly use the ",(0,i.kt)("inlineCode",{parentName:"p"},"ext-departures")," as ",(0,i.kt)("em",{parentName:"p"},"inputId")," in the ",(0,i.kt)("inlineCode",{parentName:"p"},"deduplicate-departures")," action, and rename the Action as ",(0,i.kt)("inlineCode",{parentName:"p"},"download-deduplicate-departures"),".\nThe deduplicate action expects a DataFrame as input. Since our ",(0,i.kt)("inlineCode",{parentName:"p"},"CustomWebserviceDataObject")," delivers that, there is no need for an intermediate step anymore.",(0,i.kt)("br",{parentName:"p"}),"\n","After you've changed that, the first transformer has to be rewritten as well, since the input has changed.\nPlease replace it with the implementation below"),(0,i.kt)("pre",null,(0,i.kt)("code",{parentName:"pre"},"{\n  type = SQLDfTransformer\n  code = \"select ext_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from ext_departures\"\n}\n")),(0,i.kt)("p",null,"The old Action ",(0,i.kt)("inlineCode",{parentName:"p"},"download-departures")," and the DataObject ",(0,i.kt)("inlineCode",{parentName:"p"},"stg-departures")," can be deleted, as it's not needed anymore."),(0,i.kt)("p",null,"At the end, your config file should look something like ",(0,i.kt)("a",{target:"_blank",href:n(1630).Z},"this")," and the CustomWebserviceDataObject code like ",(0,i.kt)("a",{target:"_blank",href:n(1262).Z},"this"),"."))}m.isMDXComponent=!0},1262:function(e,t,n){t.Z=n.p+"assets/files/CustomWebserviceDataObject-1-d5a9668606d763f38f3639dbcd319a17.scala"},1630:function(e,t,n){t.Z=n.p+"assets/files/application-download-part3-custom-webservice-b66a2c64b5a8d6b2b9f8f28a70aac629.conf"},5471:function(e,t,n){t.Z=n.p+"assets/files/application-historical-part2-843a2c2778f891966427eaf1258fa99e.conf"}}]);