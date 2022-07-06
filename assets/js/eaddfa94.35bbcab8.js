"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[5134],{3905:(e,t,n)=>{n.d(t,{Zo:()=>p,kt:()=>h});var a=n(7294);function o(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function r(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var a=Object.getOwnPropertySymbols(e);t&&(a=a.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,a)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?r(Object(n),!0).forEach((function(t){o(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):r(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function s(e,t){if(null==e)return{};var n,a,o=function(e,t){if(null==e)return{};var n,a,o={},r=Object.keys(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||(o[n]=e[n]);return o}(e,t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);for(a=0;a<r.length;a++)n=r[a],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(o[n]=e[n])}return o}var l=a.createContext({}),c=function(e){var t=a.useContext(l),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},p=function(e){var t=c(e.components);return a.createElement(l.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return a.createElement(a.Fragment,{},t)}},d=a.forwardRef((function(e,t){var n=e.components,o=e.mdxType,r=e.originalType,l=e.parentName,p=s(e,["components","mdxType","originalType","parentName"]),d=c(n),h=o,m=d["".concat(l,".").concat(h)]||d[h]||u[h]||r;return n?a.createElement(m,i(i({ref:t},p),{},{components:n})):a.createElement(m,i({ref:t},p))}));function h(e,t){var n=arguments,o=t&&t.mdxType;if("string"==typeof e||o){var r=n.length,i=new Array(r);i[0]=d;var s={};for(var l in t)hasOwnProperty.call(t,l)&&(s[l]=t[l]);s.originalType=e,s.mdxType="string"==typeof e?e:o,i[1]=s;for(var c=2;c<r;c++)i[c]=n[c];return a.createElement.apply(null,i)}return a.createElement.apply(null,n)}d.displayName="MDXCreateElement"},8911:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>i,default:()=>p,frontMatter:()=>r,metadata:()=>s,toc:()=>l});var a=n(7462),o=(n(7294),n(3905));const r={title:"Get Airports"},i=void 0,s={unversionedId:"getting-started/part-1/get-airports",id:"getting-started/part-1/get-airports",title:"Get Airports",description:"Goal",source:"@site/docs/getting-started/part-1/get-airports.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/get-airports",permalink:"/docs/getting-started/part-1/get-airports",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/get-airports.md",tags:[],version:"current",frontMatter:{title:"Get Airports"},sidebar:"docs",previous:{title:"Get Departures",permalink:"/docs/getting-started/part-1/get-departures"},next:{title:"Select Columns",permalink:"/docs/getting-started/part-1/select-columns"}},l=[{value:"Goal",id:"goal",children:[],level:2},{value:"Solution",id:"solution",children:[],level:2},{value:"Mess Up the Solution",id:"mess-up-the-solution",children:[],level:2},{value:"Try fixing it",id:"try-fixing-it",children:[],level:2}],c={toc:l};function p(e){let{components:t,...r}=e;return(0,o.kt)("wrapper",(0,a.Z)({},c,r,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"goal"},"Goal"),(0,o.kt)("p",null,"In this step, we will download airports master data from the website described in ",(0,o.kt)("a",{parentName:"p",href:"../get-input-data"},"Inputs"),' using Smart Data Lake Builder.\nBecause this step is very similar to the previous one, we will make some "mistake" on purpose to demonstrate how to deal with config errors.'),(0,o.kt)("p",null,"Just like in the previous step, we need one action and two DataObjects.\nExcept for the object and action names, the config to add here is almost identical to the previous step."),(0,o.kt)("p",null,'You are welcome to try to implement it yourself before continuing.\nJust as in the previous step, you can use "download" as feed name.'),(0,o.kt)("h2",{id:"solution"},"Solution"),(0,o.kt)("p",null,"You should now have a file similar to ",(0,o.kt)("a",{target:"_blank",href:n(3656).Z},"this")," one.\nThe only notable difference is that you had to use the type ",(0,o.kt)("strong",{parentName:"p"},"CsvFileDataObject")," for the airports.csv file,\nsince this is what the second webservice answers with.\nNote that you would not get an error at this point if you had chosen another file format.\nSince we use ",(0,o.kt)("em",{parentName:"p"},"FileTransferAction")," in both cases, the files are copied without the content being interpreted yet."),(0,o.kt)("p",null,"You can start the same ",(0,o.kt)("em",{parentName:"p"},"docker run")," command as before and you should see that both directories\n",(0,o.kt)("em",{parentName:"p"},"stg-airports")," and ",(0,o.kt)("em",{parentName:"p"},"stg-departures")," have new files now.\nNotice that since both actions have the same feed, the option ",(0,o.kt)("em",{parentName:"p"},"--feed-sel download")," executes both of them."),(0,o.kt)("h2",{id:"mess-up-the-solution"},"Mess Up the Solution"),(0,o.kt)("p",null,"Now let's see what happens when things don't go as planned.\nFor that, replace your config file with the contents of ",(0,o.kt)("a",{target:"_blank",href:n(8658).Z},"this")," file.\nWhen you start the ",(0,o.kt)("em",{parentName:"p"},"docker run")," command again, you will see two errors:"),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},'The name of the DataObject "NOPEext-departures" does not match with the inputId of the action download-departures.\nThis is a very common error and the stacktrace should help you to quickly find and correct it')),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'Exception in thread "main" io.smartdatalake.config.ConfigurationException: (Action~download-departures) [] key not found: DataObject~ext-departures\n')),(0,o.kt)("p",null,"As noted before, SDL will often use Action-IDs and DataObject-IDs to communicate where to look in your configuration files."),(0,o.kt)("ol",{start:2},(0,o.kt)("li",{parentName:"ol"},"An unknown DataObject type was used. In this example, stg-airports was assigned the type UnicornFileDataObject, which does not exist.")),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'Exception in thread "main" io.smartdatalake.config.ConfigurationException: (DataObject~stg-airports) ClassNotFoundException: io.smartdatalake.workflow.dataobject.UnicornFileDataObject\n')),(0,o.kt)("p",null,"Internally, the types you choose are represented by Scala Classes.\nThese classes define all characteristics of a DataObject and all it's parameters, i.e. the url we defined in our WebserviceFileDataObject.\nThis also explains why you get a ",(0,o.kt)("em",{parentName:"p"},"ClassNotFoundException")," in this case."),(0,o.kt)("h2",{id:"try-fixing-it"},"Try fixing it"),(0,o.kt)("p",null,"Try to fix one of the errors and keep the other one to see what happens: Nothing.\nWhy is that? "),(0,o.kt)("p",null,"SDL validates your configuration file(s) before executing it's contents.\nIf the configuration does not make sense, it will abort before executing anything to minimize the chance that you'll end up in an inconsistent state."),(0,o.kt)("div",{className:"admonition admonition-tip alert alert--success"},(0,o.kt)("div",{parentName:"div",className:"admonition-heading"},(0,o.kt)("h5",{parentName:"div"},(0,o.kt)("span",{parentName:"h5",className:"admonition-icon"},(0,o.kt)("svg",{parentName:"span",xmlns:"http://www.w3.org/2000/svg",width:"12",height:"16",viewBox:"0 0 12 16"},(0,o.kt)("path",{parentName:"svg",fillRule:"evenodd",d:"M6.5 0C3.48 0 1 2.19 1 5c0 .92.55 2.25 1 3 1.34 2.25 1.78 2.78 2 4v1h5v-1c.22-1.22.66-1.75 2-4 .45-.75 1-2.08 1-3 0-2.81-2.48-5-5.5-5zm3.64 7.48c-.25.44-.47.8-.67 1.11-.86 1.41-1.25 2.06-1.45 3.23-.02.05-.02.11-.02.17H5c0-.06 0-.13-.02-.17-.2-1.17-.59-1.83-1.45-3.23-.2-.31-.42-.67-.67-1.11C2.44 6.78 2 5.65 2 5c0-2.2 2.02-4 4.5-4 1.22 0 2.36.42 3.22 1.19C10.55 2.94 11 3.94 11 5c0 .66-.44 1.78-.86 2.48zM4 14h5c-.23 1.14-1.3 2-2.5 2s-2.27-.86-2.5-2z"}))),"tip")),(0,o.kt)("div",{parentName:"div",className:"admonition-content"},(0,o.kt)("p",{parentName:"div"},"During validation, the whole configuration is checked, not just the parts you are trying to execute.\nIf you have large configuration files, it can sometimes be confusing to see an error and realize that\nit's not on the part you are currently working on but in a different section."))),(0,o.kt)("p",null,"SDL is built to detect configuration errors as early as possible (early-validation). It does this by going through several phases. "),(0,o.kt)("ol",null,(0,o.kt)("li",{parentName:"ol"},"Validate configuration",(0,o.kt)("br",{parentName:"li"}),"validate superfluous attributes, missing mandatory attributes, attribute content and consistency when referencing other configuration objects."),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("em",{parentName:"li"},"Prepare")," phase",(0,o.kt)("br",{parentName:"li"}),"validate preconditions, e.g. connections and existence of tables and directories."),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("em",{parentName:"li"},"Init")," phase",(0,o.kt)("br",{parentName:"li"}),"executes the whole feed ",(0,o.kt)("em",{parentName:"li"},"without any data")," to spot incompatibilities between the Data Objects that cannot be spotted\nby just looking at the config file. For example a column which doesn't exist but is referenced in a later Action will cause the init phase to fail."),(0,o.kt)("li",{parentName:"ol"},(0,o.kt)("em",{parentName:"li"},"Exec")," phase",(0,o.kt)("br",{parentName:"li"}),"only if all previous phases have been passed successfully, execution is started.")),(0,o.kt)("p",null,'When running SDL, you can clearly find "prepare", "init" and "exec" steps for every Action in the logs.'),(0,o.kt)("p",null,"See ",(0,o.kt)("a",{parentName:"p",href:"/docs/reference/executionPhases"},"this page")," for a detailed description on the execution phases of SDL."),(0,o.kt)("p",null,"Now is a good time to fix both errors in your configuration file and execute the action again."),(0,o.kt)("p",null,"Early-validation is a core feature of SDL and will become more and more valuable with the increasing complexity of your data pipelines.\nSpeaking of increasing complexity: In the next step, we will begin transforming our data."))}p.isMDXComponent=!0},8658:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/application-part1-download-errors-be368786182101f149c2e1119c3f5dfa.conf"},3656:(e,t,n)=>{n.d(t,{Z:()=>a});const a=n.p+"assets/files/application-part1-download-b129c4e7b7f455917f3f1941b51b191b.conf"}}]);