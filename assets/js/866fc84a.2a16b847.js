"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[9233],{3905:(e,t,a)=>{a.d(t,{Zo:()=>p,kt:()=>u});var r=a(7294);function n(e,t,a){return t in e?Object.defineProperty(e,t,{value:a,enumerable:!0,configurable:!0,writable:!0}):e[t]=a,e}function o(e,t){var a=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),a.push.apply(a,r)}return a}function i(e){for(var t=1;t<arguments.length;t++){var a=null!=arguments[t]?arguments[t]:{};t%2?o(Object(a),!0).forEach((function(t){n(e,t,a[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(a)):o(Object(a)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(a,t))}))}return e}function l(e,t){if(null==e)return{};var a,r,n=function(e,t){if(null==e)return{};var a,r,n={},o=Object.keys(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||(n[a]=e[a]);return n}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)a=o[r],t.indexOf(a)>=0||Object.prototype.propertyIsEnumerable.call(e,a)&&(n[a]=e[a])}return n}var c=r.createContext({}),s=function(e){var t=r.useContext(c),a=t;return e&&(a="function"==typeof e?e(t):i(i({},t),e)),a},p=function(e){var t=s(e.components);return r.createElement(c.Provider,{value:t},e.children)},d={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var a=e.components,n=e.mdxType,o=e.originalType,c=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),m=s(a),u=n,b=m["".concat(c,".").concat(u)]||m[u]||d[u]||o;return a?r.createElement(b,i(i({ref:t},p),{},{components:a})):r.createElement(b,i({ref:t},p))}));function u(e,t){var a=arguments,n=t&&t.mdxType;if("string"==typeof e||n){var o=a.length,i=new Array(o);i[0]=m;var l={};for(var c in t)hasOwnProperty.call(t,c)&&(l[c]=t[c]);l.originalType=e,l.mdxType="string"==typeof e?e:n,i[1]=l;for(var s=2;s<o;s++)i[s]=a[s];return r.createElement.apply(null,i)}return r.createElement.apply(null,a)}m.displayName="MDXCreateElement"},3190:(e,t,a)=>{a.r(t),a.d(t,{assets:()=>c,contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var r=a(7462),n=(a(7294),a(3905));const o={title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",slug:"sdl-databricks",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Databricks","Cloud"],hide_table_of_contents:!1},i=void 0,l={permalink:"/blog/sdl-databricks",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/blog/2022-04-07-SDL_databricks/2022-04-07-Databricks.md",source:"@site/blog/2022-04-07-SDL_databricks/2022-04-07-Databricks.md",title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",date:"2022-04-07T00:00:00.000Z",formattedDate:"April 7, 2022",tags:[{label:"Databricks",permalink:"/blog/tags/databricks"},{label:"Cloud",permalink:"/blog/tags/cloud"}],readingTime:5.74,truncated:!0,authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],frontMatter:{title:"Deployment on Databricks",description:"A brief example of deploying SDL on Databricks",slug:"sdl-databricks",authors:[{name:"Mandes Sch\xf6nherr",title:"Dr.sc.nat.",url:"https://github.com/mand35"}],tags:["Databricks","Cloud"],hide_table_of_contents:!1},nextItem:{title:"Combine Spark and Snowpark to ingest and transform data in one pipeline",permalink:"/blog/sdl-snowpark"}},c={authorsImageUrls:[void 0]},s=[],p={toc:s};function d(e){let{components:t,...a}=e;return(0,n.kt)("wrapper",(0,r.Z)({},p,a,{components:t,mdxType:"MDXLayout"}),(0,n.kt)("p",null,"Many analytics applications are ported to the cloud, Data Lakes and Lakehouses in the cloud becoming more and more popular.\nThe ",(0,n.kt)("a",{parentName:"p",href:"https://databricks.com"},"Databricks")," platform provides an easy accessible and easy configurable way to implement a modern analytics platform.\nSmart Data Lake Builder on the other hand provides an open source, portable automation tool to load and transform the data."),(0,n.kt)("p",null,"In this article the deployment of Smart Data Lake Builder (SDLB) on ",(0,n.kt)("a",{parentName:"p",href:"https://databricks.com"},"Databricks")," is described."))}d.isMDXComponent=!0}}]);