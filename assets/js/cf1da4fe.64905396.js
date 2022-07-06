"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3868],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>u});var n=a(7294);function o(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function r(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,n)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?r(Object(a),!0).forEach((function(t){o(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):r(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,n,o=function(e,t){if(null==e)return{};var a,n,o={},r=Object.keys(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||(o[a]=e[a]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(n=0;n<r.length;n++)a=r[n],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(o[a]=e[a])}return o}var s=n.createContext({}),d=function(e){var t=n.useContext(s),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},p=function(e){var t=d(e.components);return n.createElement(s.Provider,{value:t},e.children)},c={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},m=n.forwardRef((function(e,t){var a=e.components,o=e.mdxType,r=e.originalType,s=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),m=d(a),u=o,h=m["".concat(s,".").concat(u)]||m[u]||c[u]||r;return a?n.createElement(h,i(i({ref:t},p),{},{components:a})):n.createElement(h,i({ref:t},p))}));function u(e,t){var a=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=a.length,i=new Array(r);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l.mdxType="string"==typeof e?e:o,i[1]=l;for(var d=2;d<r;d++)i[d]=a[d];return n.createElement.apply(null,i)}return n.createElement.apply(null,a)}m.displayName="MDXCreateElement"},8215:(e,t,a)=>{a.d(t,{Z:()=>o});var n=a(7294);const o=function(e){let{children:t,hidden:a,className:o}=e;return n.createElement("div",{role:"tabpanel",hidden:a,className:o},t)}},9877:(e,t,a)=>{a.d(t,{Z:()=>p});var n=a(7462),o=a(7294),r=a(2389),i=a(5773),l=a(6010);const s="tabItem_LplD";function d(e){var t,a,r;const{lazy:d,block:p,defaultValue:c,values:m,groupId:u,className:h}=e,k=o.Children.map(e.children,(e=>{if((0,o.isValidElement)(e)&&void 0!==e.props.value)return e;throw new Error("Docusaurus error: Bad <Tabs> child <"+("string"==typeof e.type?e.type:e.type.name)+'>: all children of the <Tabs> component should be <TabItem>, and every <TabItem> should have a unique "value" prop.')})),f=null!=m?m:k.map((e=>{let{props:{value:t,label:a,attributes:n}}=e;return{value:t,label:a,attributes:n}})),v=(0,i.lx)(f,((e,t)=>e.value===t.value));if(v.length>0)throw new Error('Docusaurus error: Duplicate values "'+v.map((e=>e.value)).join(", ")+'" found in <Tabs>. Every value needs to be unique.');const g=null===c?c:null!=(t=null!=c?c:null==(a=k.find((e=>e.props.default)))?void 0:a.props.value)?t:null==(r=k[0])?void 0:r.props.value;if(null!==g&&!f.some((e=>e.value===g)))throw new Error('Docusaurus error: The <Tabs> has a defaultValue "'+g+'" but none of its children has the corresponding value. Available values are: '+f.map((e=>e.value)).join(", ")+". If you intend to show no default tab, use defaultValue={null} instead.");const{tabGroupChoices:b,setTabGroupChoices:w}=(0,i.UB)(),[N,y]=(0,o.useState)(g),D=[],{blockElementScrollPositionUntilNextRender:C}=(0,i.o5)();if(null!=u){const e=b[u];null!=e&&e!==N&&f.some((t=>t.value===e))&&y(e)}const x=e=>{const t=e.currentTarget,a=D.indexOf(t),n=f[a].value;n!==N&&(C(t),y(n),null!=u&&w(u,n))},T=e=>{var t;let a=null;switch(e.key){case"ArrowRight":{const t=D.indexOf(e.currentTarget)+1;a=D[t]||D[0];break}case"ArrowLeft":{const t=D.indexOf(e.currentTarget)-1;a=D[t]||D[D.length-1];break}}null==(t=a)||t.focus()};return o.createElement("div",{className:"tabs-container"},o.createElement("ul",{role:"tablist","aria-orientation":"horizontal",className:(0,l.Z)("tabs",{"tabs--block":p},h)},f.map((e=>{let{value:t,label:a,attributes:r}=e;return o.createElement("li",(0,n.Z)({role:"tab",tabIndex:N===t?0:-1,"aria-selected":N===t,key:t,ref:e=>D.push(e),onKeyDown:T,onFocus:x,onClick:x},r,{className:(0,l.Z)("tabs__item",s,null==r?void 0:r.className,{"tabs__item--active":N===t})}),null!=a?a:t)}))),d?(0,o.cloneElement)(k.filter((e=>e.props.value===N))[0],{className:"margin-vert--md"}):o.createElement("div",{className:"margin-vert--md"},k.map(((e,t)=>(0,o.cloneElement)(e,{key:t,hidden:e.props.value!==N})))))}function p(e){const t=(0,r.Z)();return o.createElement(d,(0,n.Z)({key:String(t)},e))}},8906:(e,t,a)=>{a.r(t),a.d(t,{contentTitle:()=>s,default:()=>m,frontMatter:()=>l,metadata:()=>d,toc:()=>p});var n=a(7462),o=(a(7294),a(3905)),r=a(9877),i=a(8215);const l={title:"Delta Lake - a better data format"},s=void 0,d={unversionedId:"getting-started/part-2/delta-lake-format",id:"getting-started/part-2/delta-lake-format",title:"Delta Lake - a better data format",description:"Goal",source:"@site/docs/getting-started/part-2/delta-lake-format.md",sourceDirName:"getting-started/part-2",slug:"/getting-started/part-2/delta-lake-format",permalink:"/docs/getting-started/part-2/delta-lake-format",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-2/delta-lake-format.md",tags:[],version:"current",frontMatter:{title:"Delta Lake - a better data format"},sidebar:"docs",previous:{title:"Industrializing our data pipeline",permalink:"/docs/getting-started/part-2/industrializing"},next:{title:"Keeping historical data",permalink:"/docs/getting-started/part-2/historical-data"}},p=[{value:"Goal",id:"goal",children:[],level:2},{value:"File formats",id:"file-formats",children:[],level:2},{value:"Catalog",id:"catalog",children:[],level:2},{value:"Transactions",id:"transactions",children:[],level:2},{value:"DeltaLakeTableDataObject",id:"deltalaketabledataobject",children:[],level:2},{value:"Reading Delta Lake Format with Spark",id:"reading-delta-lake-format-with-spark",children:[],level:2}],c={toc:p};function m(e){let{components:t,...l}=e;return(0,o.kt)("wrapper",(0,n.Z)({},c,l,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"goal"},"Goal"),(0,o.kt)("p",null,"Up to now we have used CSV with CsvFileDataObject as file format. We will switch to a more modern data format in this step which supports a catalog, compression and even transactions."),(0,o.kt)("h2",{id:"file-formats"},"File formats"),(0,o.kt)("p",null,"Smart Data Lake Builder has built in support for many data formats and technologies.\nAn important one is storing files on a Hadoop filesystem, supporting standard file formats such as CSV, Json, Avro or Parquet.\nIn Part 1 we have used CSV through the CsvFileDataObject. CSV files can be easily checked in an editor or Excel, but the format also has many problems, e.g. support of multi-line strings or lack of data type definition.\nOften Parquet format is used, as it includes a schema definition and is very space efficient through its columnar compression."),(0,o.kt)("h2",{id:"catalog"},"Catalog"),(0,o.kt)("p",null,"Just storing files on Hadoop filesystem makes it difficult to use them in a SQL engine such as Spark SQL. You need a metadata layer on top which stores table definitions. This is also called a metastore or catalog.\nIf you start a Spark session, a configuration to connect to an external catalog can be set, or otherwise Spark creates an internal catalog for the session.\nWe could register our CSV files in this catalog by creating a table via a DDL-statement, including the definition of all columns, a path and the format of our data.\nBut you could also directly create and write into a table by using Spark Hive tables.\nSmart Data Lake Builder supports this by the HiveTableDataObject. It always uses Parquet file format in the background as a best practice, although Hive tables could also be created on top of CSV files."),(0,o.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"Hive is a Metadata layer and SQL engine on top of a Hadoop filesystem. Spark uses the metadata layer of Hive, but implements its own SQL engine."))),(0,o.kt)("h2",{id:"transactions"},"Transactions"),(0,o.kt)("p",null,"Hive tables with Parquet format are lacking transactions. This means for example that writing and reading the table at the same time could result in failure or empty results.\nIn consequence "),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"consecutive jobs need to by synchronized"),(0,o.kt)("li",{parentName:"ul"},"it's not recommended having end-user accessing the table while data processing jobs are running"),(0,o.kt)("li",{parentName:"ul"},"update and deletes are not supported")),(0,o.kt)("p",null,"There are other options like classical databases which always had a metadata layer, offer transactions but don't integrate easily with Hive metastore and cheap, scalable Hadoop file storage.\nNevertheless, Smart Data Lake Builder supports classical databases through the JdbcTableDataObject.\nFortunately there is a new technology called Delta Lake, see also ",(0,o.kt)("a",{parentName:"p",href:"https://delta.io/"},"delta.io"),". It integrates into a Hive metastore, supports transactions and stores Parquet files and a transaction log on hadoop filesystems.\nSmart Data Lake Builder supports this by the DeltaLakeTableDataObject, and this is what we are going to use for our airport and departure data now."),(0,o.kt)("h2",{id:"deltalaketabledataobject"},"DeltaLakeTableDataObject"),(0,o.kt)("p",null,"Switching to Delta Lake format is easy with Smart Data Lake Builder, just replace ",(0,o.kt)("inlineCode",{parentName:"p"},"CsvFileDataObject")," with ",(0,o.kt)("inlineCode",{parentName:"p"},"DeltaLakeTableDataObject")," and define the table's db and name.\nLet's start by changing the existing definition for ",(0,o.kt)("inlineCode",{parentName:"p"},"int-airports"),":"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'int-airports {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table = {\n        db = default\n        name = int_airports\n    }\n}\n')),(0,o.kt)("p",null,"Then create a new, similar data object ",(0,o.kt)("inlineCode",{parentName:"p"},"int-departures"),": "),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'int-departures {\n    type = DeltaLakeTableDataObject\n    path = "~{id}"\n    table = {\n        db = default\n        name = int_departures\n    }\n}\n')),(0,o.kt)("p",null,"Next, create a new action ",(0,o.kt)("inlineCode",{parentName:"p"},"prepare-departures")," in the ",(0,o.kt)("inlineCode",{parentName:"p"},"actions")," section to fill the new table with the data:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},"prepare-departures {\n    type = CopyAction\n    inputId = stg-departures\n    outputId = int-departures\n    metadata {\n        feed = compute\n    }\n}\n")),(0,o.kt)("p",null,"Finally, adapt the action definition for ",(0,o.kt)("inlineCode",{parentName:"p"},"join-departures-airports"),":"),(0,o.kt)("ul",null,(0,o.kt)("li",{parentName:"ul"},"change ",(0,o.kt)("inlineCode",{parentName:"li"},"stg-departures")," to ",(0,o.kt)("inlineCode",{parentName:"li"},"int-departures")," in inputIds"),(0,o.kt)("li",{parentName:"ul"},"change ",(0,o.kt)("inlineCode",{parentName:"li"},"stg_departures")," to ",(0,o.kt)("inlineCode",{parentName:"li"},"int_departures")," in the first SQLDfsTransformer (watch out, you need to replace the string 4 times)")),(0,o.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"Explanation")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("ul",{parentName:"div"},(0,o.kt)("li",{parentName:"ul"},"We changed ",(0,o.kt)("inlineCode",{parentName:"li"},"int-airports")," from CSV to Delta Lake format"),(0,o.kt)("li",{parentName:"ul"},"Created an additional table ",(0,o.kt)("inlineCode",{parentName:"li"},"int-departures")),(0,o.kt)("li",{parentName:"ul"},"Created an action ",(0,o.kt)("inlineCode",{parentName:"li"},"prepare-departures"),"  to fill the new integration layer table ",(0,o.kt)("inlineCode",{parentName:"li"},"int-departures")),(0,o.kt)("li",{parentName:"ul"},"Adapted the existing action ",(0,o.kt)("inlineCode",{parentName:"li"},"join-departures-airports")," to use the new table ",(0,o.kt)("inlineCode",{parentName:"li"},"int-departures"))))),(0,o.kt)("p",null,"To run our data pipeline, first delete the data directory - otherwise DeltaLakeTableDataObject will fail because of existing files in different format.\nThen you can execute the usual ",(0,o.kt)("em",{parentName:"p"},"docker run")," command for all feeds:"),(0,o.kt)(r.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"mkdir -f data\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel 'download*'\ndocker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel '^(?!download).*'\n"))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"mkdir -f data\npodman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel 'download*'\npodman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel '^(?!download).*'\n")))),(0,o.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"info")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"Why two separate commands?",(0,o.kt)("br",{parentName:"p"}),"\n","Because you deleted all data first.   "),(0,o.kt)("p",{parentName:"div"},"Remember from part 1 that we either need to define a schema for our downloaded files or we need to execute the download steps separately on the first run.\nThe first command only executes the download steps, the second command executes everything but the download steps (regex with negative lookahead).\nSee ",(0,o.kt)("a",{parentName:"p",href:"/docs/getting-started/troubleshooting/common-problems"},"Common Problems")," for more Info."))),(0,o.kt)("p",null,"Getting an error like ",(0,o.kt)("inlineCode",{parentName:"p"},"io.smartdatalake.util.webservice.WebserviceException: Read timed out"),"? Check the list of ",(0,o.kt)("a",{parentName:"p",href:"../troubleshooting/common-problems"},"Common Problems")," for a workaround."),(0,o.kt)("h2",{id:"reading-delta-lake-format-with-spark"},"Reading Delta Lake Format with Spark"),(0,o.kt)("p",null,"Checking our results gets more complicated now - we can't just open delta lake format in a text editor like we used to do for CSV files.\nWe could now use SQL to query our results, that would be even better.\nOne option is to use a Spark session, i.e. by starting a spark-shell.\nBut state-of-the-art is to use notebooks like Jupyter for this.\nOne of the most advanced notebooks for Scala code we found is Polynote, see ",(0,o.kt)("a",{parentName:"p",href:"https://polynote.org/"},"polynote.org"),"."),(0,o.kt)("p",null,"We will now start Polynote in a docker container, and an external Metastore (Derby database) in another container to share the catalog between our experiments and the notebook.\nTo do so you need to add additional files to the project. Change to the projects root directory and ",(0,o.kt)("strong",{parentName:"p"},"unzip part2.additional-files.zip")," into the project's root directoy, then run the following commands in the projects root directory:"),(0,o.kt)(r.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"docker-compose build\nmkdir -p data/_metastore\ndocker-compose up\n"))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"podman-compose build\nmkdir -p data/_metastore\npodman-compose up\n")))),(0,o.kt)("p",null,"This might take multiple minutes.\nYou should now be able to access Polynote at ",(0,o.kt)("inlineCode",{parentName:"p"},"localhost:8192"),". "),(0,o.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"Docker on Windows")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"If you use Windows, please read our note on ",(0,o.kt)("a",{parentName:"p",href:"../troubleshooting/docker-on-windows"},"Docker for Windows"),"."))),(0,o.kt)("p",null,'But when you walk through the prepared notebook "SelectingData", you won\'t see any tables and data yet.\nCan you guess why?',(0,o.kt)("br",{parentName:"p"}),"\n","This is because your last pipeline run used an internal metastore, and not the external metastore we started with docker-compose yet.\nTo configure Spark to use our external metastore, add the following spark properties to the application.conf under global.spark-options.\nYou probably don't have a global section in your application.conf yet, so here is the full block you need to add at the top of the file:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'global {\n  spark-options {\n    "spark.hadoop.javax.jdo.option.ConnectionURL" = "jdbc:derby://metastore:1527/db;create=true"\n    "spark.hadoop.javax.jdo.option.ConnectionDriverName" = "org.apache.derby.jdbc.ClientDriver"\n    "spark.hadoop.javax.jdo.option.ConnectionUserName" = "sa"\n    "spark.hadoop.javax.jdo.option.ConnectionPassword" = "1234"\n  }\n}\n')),(0,o.kt)("p",null,"This instructs Spark to use the external metastore you started with docker-compose.\nYour Smart Data Lake container doesn't have access to the other containers just yet.\nSo when you run your data pipeline again, you need to add a parameter ",(0,o.kt)("inlineCode",{parentName:"p"},"--network"),"/",(0,o.kt)("inlineCode",{parentName:"p"},"--pod")," to join the virtual network where the metastore is located:"),(0,o.kt)(r.Z,{groupId:"docker-podman-switch",defaultValue:"docker",values:[{label:"Docker",value:"docker"},{label:"Podman",value:"podman"}],mdxType:"Tabs"},(0,o.kt)(i.Z,{value:"docker",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"docker run --hostname localhost --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest -c /mnt/config --feed-sel '.*'\n"))),(0,o.kt)(i.Z,{value:"podman",mdxType:"TabItem"},(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre",className:"language-jsx"},"podman run --hostname localhost --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest -c /mnt/config --feed-sel '.*'\n")))),(0,o.kt)("div",{className:"admonition admonition-info alert alert--info"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"14",height:"16",viewBox:"0 0 14 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M7 2.3c3.14 0 5.7 2.56 5.7 5.7s-2.56 5.7-5.7 5.7A5.71 5.71 0 0 1 1.3 8c0-3.14 2.56-5.7 5.7-5.7zM7 1C3.14 1 0 4.14 0 8s3.14 7 7 7 7-3.14 7-7-3.14-7-7-7zm1 3H6v5h2V4zm0 6H6v2h2v-2z"}))),"Hostname specification")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"Without specifying the hostname, the containter name (by default the docker/podman container ID) can not be resolved to localhost. If you need to name your container differently, the following arguments can be used alternatively: ",(0,o.kt)("inlineCode",{parentName:"p"},"--hostname myhost --add-host myhost:127.0.0.1 -rm ...")))),(0,o.kt)("p",null,"After you run your data pipeline again, you should now be able to see our DataObjects data in Polynote.\nNo need to restart Polynote, just open it again and run all cells.\n",(0,o.kt)("a",{target:"_blank",href:a(6896).Z},"This")," is how the final configuration file should look like. Feel free to play around."),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"Delta Lake tuning")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"You might have seen that our data pipeline with DeltaTableDataObject runs a Spark stage with 50 tasks several times.\nThis is delta lake reading it's transaction log with Spark. For our data volume, 50 tasks are way too much.\nYou can reduce the number of snapshot partitions to speed up the execution by setting the following Spark property in your ",(0,o.kt)("inlineCode",{parentName:"p"},"application.conf")," under ",(0,o.kt)("inlineCode",{parentName:"p"},"global.spark-options"),":"),(0,o.kt)("pre",{parentName:"div"},(0,o.kt)("code",{parentName:"pre"},'"spark.databricks.delta.snapshotPartitions" = 2\n')))),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"Spark UI from Polynote")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"On the right side of Polynote you find a link to the Spark UI for the current notebooks Spark session.\nIf it doesn't work, try to replace 127.0.0.1 with localhost. If it still doesn't work, replace with IP address of WSL (",(0,o.kt)("inlineCode",{parentName:"p"},"wsl hostname -I"),"). "))),(0,o.kt)("p",null,"In the next step, we are going to take a look at keeping historical data..."))}m.isMDXComponent=!0},6896:(e,t,a)=>{a.d(t,{Z:()=>n});const n=a.p+"assets/files/application-part2-deltalake-d1d7d6eef2fc2f225f2f87fff7ec0481.conf"}}]);