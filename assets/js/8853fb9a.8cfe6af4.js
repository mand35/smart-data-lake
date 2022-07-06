"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[2816],{3905:(e,t,n)=>{n.d(t,{Zo:()=>c,kt:()=>m});var i=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);t&&(i=i.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,i)}return n}function r(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,i,a=function(e,t){if(null==e)return{};var n,i,a={},o=Object.keys(e);for(i=0;i<o.length;i++)n=o[i],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(i=0;i<o.length;i++)n=o[i],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var l=i.createContext({}),d=function(e){var t=i.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):r(r({},t),e)),n},c=function(e){var t=d(e.components);return i.createElement(l.Provider,{value:t},e.children)},p={inlineCode:"code",wrapper:function(e){var t=e.children;return i.createElement(i.Fragment,{},t)}},u=i.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),u=d(n),m=a,f=u["".concat(l,".").concat(m)]||u[m]||p[m]||o;return n?i.createElement(f,r(r({ref:t},c),{},{components:n})):i.createElement(f,r({ref:t},c))}));function m(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,r=new Array(o);r[0]=u;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:a,r[1]=s;for(var d=2;d<o;d++)r[d]=n[d];return i.createElement.apply(null,r)}return i.createElement.apply(null,n)}u.displayName="MDXCreateElement"},6679:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>r,default:()=>c,frontMatter:()=>o,metadata:()=>s,toc:()=>l});var i=n(7462),a=(n(7294),n(3905));const o={id:"executionModes",title:"Execution Modes"},r=void 0,s={unversionedId:"reference/executionModes",id:"reference/executionModes",title:"Execution Modes",description:"This page is under review and currently not visible in the menu.",source:"@site/docs/reference/executionModes.md",sourceDirName:"reference",slug:"/reference/executionModes",permalink:"/docs/reference/executionModes",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/reference/executionModes.md",tags:[],version:"current",frontMatter:{id:"executionModes",title:"Execution Modes"}},l=[{value:"Execution modes",id:"execution-modes",children:[{value:"Fixed partition values filter",id:"fixed-partition-values-filter",children:[],level:3},{value:"PartitionDiffMode: Dynamic partition values filter",id:"partitiondiffmode-dynamic-partition-values-filter",children:[],level:3},{value:"SparkStreamingMode: Incremental load",id:"sparkstreamingmode-incremental-load",children:[],level:3},{value:"SparkIncrementalMode: Incremental Load",id:"sparkincrementalmode-incremental-load",children:[],level:3},{value:"FailIfNoPartitionValuesMode",id:"failifnopartitionvaluesmode",children:[],level:3},{value:"ProcessAllMode",id:"processallmode",children:[],level:3},{value:"CustomPartitionMode",id:"custompartitionmode",children:[],level:3}],level:2},{value:"Execution Condition",id:"execution-condition",children:[],level:2}],d={toc:l};function c(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,i.Z)({},d,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("div",{className:"admonition admonition-warning alert alert--danger"},(0,a.kt)("div",{parentName:"div",className:"admonition-heading"},(0,a.kt)("h5",{parentName:"div"},(0,a.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,a.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,a.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M5.05.31c.81 2.17.41 3.38-.52 4.31C3.55 5.67 1.98 6.45.9 7.98c-1.45 2.05-1.7 6.53 3.53 7.7-2.2-1.16-2.67-4.52-.3-6.61-.61 2.03.53 3.33 1.94 2.86 1.39-.47 2.3.53 2.27 1.67-.02.78-.31 1.44-1.13 1.81 3.42-.59 4.78-3.42 4.78-5.56 0-2.84-2.53-3.22-1.25-5.61-1.52.13-2.03 1.13-1.89 2.75.09 1.08-1.02 1.8-1.86 1.33-.67-.41-.66-1.19-.06-1.78C8.18 5.31 8.68 2.45 5.05.32L5.03.3l.02.01z"}))),"warning")),(0,a.kt)("div",{parentName:"div",className:"admonition-content"},(0,a.kt)("p",{parentName:"div"},"This page is under review and currently not visible in the menu."))),(0,a.kt)("h2",{id:"execution-modes"},"Execution modes"),(0,a.kt)("p",null,"Execution modes select the data to be processed. By default, if you start SmartDataLakeBuilder, there is no filter applied. This means every Action reads all data from its input DataObjects."),(0,a.kt)("p",null,'You can set an execution mode by defining attribute "executionMode" of an Action. Define the chosen ExecutionMode by setting type as follows:'),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"executionMode {\n  type = PartitionDiffMode\n  attribute1 = ...\n}\n")),(0,a.kt)("h3",{id:"fixed-partition-values-filter"},"Fixed partition values filter"),(0,a.kt)("p",null,"You can apply a filter manually by specifying parameter ",(0,a.kt)("inlineCode",{parentName:"p"},"--partition-values")," or ",(0,a.kt)("inlineCode",{parentName:"p"},"--multi-partition-values")," on the command line. The partition values specified are passed to all start-Actions of a DAG and filtered for every input DataObject by its defined partition columns.\nOn execution every Action takes the partition values of the input and filters them again for every output DataObject by its defined partition columns, which serve again as partition values for the input of the next Action.\nNote that during execution of the dag, no new partition values are added, they are only filtered. An exception is if you place a PartitionDiffMode in the middle of your pipeline, see next section."),(0,a.kt)("h3",{id:"partitiondiffmode-dynamic-partition-values-filter"},"PartitionDiffMode: Dynamic partition values filter"),(0,a.kt)("p",null,"Alternatively you can let SmartDataLakeBuilder find missing partitions and set partition values automatically by specifying execution mode PartitionDiffMode."),(0,a.kt)("p",null,"By defining the ",(0,a.kt)("strong",{parentName:"p"},"applyCondition")," attribute you can give a condition to decide at runtime if the PartitionDiffMode should be applied or not.\nDefault is to apply the PartitionDiffMode if the given partition values are empty (partition values from command line or passed from previous action).\nDefine an applyCondition by a spark sql expression working with attributes of DefaultExecutionModeExpressionData returning a boolean."),(0,a.kt)("p",null,"By defining the ",(0,a.kt)("strong",{parentName:"p"},"failCondition")," attribute you can give a condition to fail application of execution mode if true.\nIt can be used to fail a run based on expected partitions, time and so on.\nThe expression is evaluated after execution of PartitionDiffMode, amongst others there are attributes inputPartitionValues, outputPartitionValues and selectedPartitionValues to make the decision.\nDefault is that the application of the PartitionDiffMode does not fail the action. If there is no data to process, the following actions are skipped.\nDefine a failCondition by a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a boolean."),(0,a.kt)("p",null,'Example - fail if partitions are not processed in strictly increasing order of partition column "dt":'),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'  failCondition = "(size(selectedPartitionValues) > 0 and array_min(transform(selectedPartitionValues, x -&gt x.dt)) &lt array_max(transform(outputPartitionValues, x > x.dt)))"\n')),(0,a.kt)("p",null,"Sometimes the failCondition can become quite complex with multiple terms concatenated by or-logic.\nTo improve interpretabily of error messages, multiple fail conditions can be configured as array with attribute ",(0,a.kt)("strong",{parentName:"p"},"failConditions"),". For every condition you can also define a description which will be inserted into the error message."),(0,a.kt)("p",null,"Finally By defining ",(0,a.kt)("strong",{parentName:"p"},"selectExpression")," you can customize which partitions are selected.\nDefine a spark sql expression working with attributes of PartitionDiffModeExpressionData returning a Seq(Map(String,String))."),(0,a.kt)("p",null,"Example - only process the last selected partition:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'  selectExpression = "slice(selectedPartitionValues,-1,1)"\n')),(0,a.kt)("p",null,"By defining ",(0,a.kt)("strong",{parentName:"p"},"alternativeOutputId")," attribute you can define another DataObject which will be used to check for already existing data.\nThis can be used to select data to process against a DataObject later in the pipeline."),(0,a.kt)("h3",{id:"sparkstreamingmode-incremental-load"},"SparkStreamingMode: Incremental load"),(0,a.kt)("p",null,'Some DataObjects are not partitioned, but nevertheless you dont want to read all data from the input on every run. You want to load it incrementally.\nThis can be accomplished by specifying execution mode SparkStreamingMode. Under the hood it uses "Spark Structured Streaming".\nIn streaming mode this an Action with SparkStreamingMode is an asynchronous action. Its rhythm can be configured by setting triggerType and triggerTime.\nIf not in streaming mode SparkStreamingMode triggers a single microbatch by using triggerType=Once and is fully synchronized. Synchronous execution can be forced for streaming mode as well by explicitly setting triggerType=Once.\n"Spark Structured Streaming" is keeping state information about processed data. It needs a checkpointLocation configured which can be given as parameter to SparkStreamingMode.'),(0,a.kt)("p",null,'Note that "Spark Structured Streaming" needs an input DataObject supporting the creation of streaming DataFrames.\nFor the time being, only the input sources delivered with Spark Streaming are supported.\nThis are KafkaTopicDataObject and all SparkFileDataObjects, see also ',(0,a.kt)("a",{parentName:"p",href:"https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets"},"Spark StructuredStreaming"),"."),(0,a.kt)("h3",{id:"sparkincrementalmode-incremental-load"},"SparkIncrementalMode: Incremental Load"),(0,a.kt)("p",null,"As not every input DataObject supports the creation of streaming DataFrames, there is an other execution mode called SparkIncrementalMode.\nYou configure it by defining the attribute ",(0,a.kt)("strong",{parentName:"p"},"compareCol")," with a column name present in input and output DataObject.\nSparkIncrementalMode then compares the maximum values between input and output and creates a filter condition.\nOn execution the filter condition is applied to the input DataObject to load the missing increment.\nNote that compareCol needs to have a sortable datatype."),(0,a.kt)("p",null,"By defining ",(0,a.kt)("strong",{parentName:"p"},"applyCondition")," attribute you can give a condition to decide at runtime if the SparkIncrementalMode should be applied or not.\nDefault is to apply the SparkIncrementalMode. Define an applyCondition by a spark sql expression working with attributes of DefaultExecutionModeExpressionData returning a boolean."),(0,a.kt)("p",null,"By defining ",(0,a.kt)("strong",{parentName:"p"},"alternativeOutputId")," attribute you can define another DataObject which will be used to check for already existing data.\nThis can be used to select data to process against a DataObject later in the pipeline."),(0,a.kt)("h3",{id:"failifnopartitionvaluesmode"},"FailIfNoPartitionValuesMode"),(0,a.kt)("p",null,"To simply check if partition values are present and fail otherwise, configure execution mode FailIfNoPartitionValuesMode.\nThis is useful to prevent potential reprocessing of whole table through wrong usage."),(0,a.kt)("h3",{id:"processallmode"},"ProcessAllMode"),(0,a.kt)("p",null,"An execution mode which forces processing all data from it's inputs, removing partitionValues and filter conditions received from previous actions."),(0,a.kt)("h3",{id:"custompartitionmode"},"CustomPartitionMode"),(0,a.kt)("p",null,"An execution mode to create custom partition execution mode logic in scala.\nImplement trait CustomPartitionModeLogic by defining a function which receives main input&output DataObject and returns partition values to process as Seq[Map","[String,String]","]"),(0,a.kt)("h2",{id:"execution-condition"},"Execution Condition"),(0,a.kt)("p",null,"For every Action an executionCondition can be defined. The execution condition allows to define if an action is executed or skipped. The default behaviour is that an Action is skipped if at least one input SubFeed is skipped.\nDefine an executionCondition by a spark sql expression working with attributes of SubFeedsExpressionData returning a boolean.\nThe Action is skipped if the executionCondition is evaluated to false. In that case dependent actions get empty SubFeeds marked with isSkipped=true as input."),(0,a.kt)("p",null,"Example - skip Action only if input1 and input2 SubFeed are skipped:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},'  executionCondition = "!inputSubFeeds.input1.isSkipped or !inputSubFeeds.input2.isSkipped"\n')),(0,a.kt)("p",null,"Example - Always execute Action and use all existing data as input:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre"},"  executionCondition = true\n  executionMode = ProcessAllMode\n")))}c.isMDXComponent=!0}}]);