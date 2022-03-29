"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[3213],{3905:function(t,e,n){n.d(e,{Zo:function(){return c},kt:function(){return f}});var r=n(7294);function a(t,e,n){return e in t?Object.defineProperty(t,e,{value:n,enumerable:!0,configurable:!0,writable:!0}):t[e]=n,t}function i(t,e){var n=Object.keys(t);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(t);e&&(r=r.filter((function(e){return Object.getOwnPropertyDescriptor(t,e).enumerable}))),n.push.apply(n,r)}return n}function o(t){for(var e=1;e<arguments.length;e++){var n=null!=arguments[e]?arguments[e]:{};e%2?i(Object(n),!0).forEach((function(e){a(t,e,n[e])})):Object.getOwnPropertyDescriptors?Object.defineProperties(t,Object.getOwnPropertyDescriptors(n)):i(Object(n)).forEach((function(e){Object.defineProperty(t,e,Object.getOwnPropertyDescriptor(n,e))}))}return t}function l(t,e){if(null==t)return{};var n,r,a=function(t,e){if(null==t)return{};var n,r,a={},i=Object.keys(t);for(r=0;r<i.length;r++)n=i[r],e.indexOf(n)>=0||(a[n]=t[n]);return a}(t,e);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(t);for(r=0;r<i.length;r++)n=i[r],e.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(t,n)&&(a[n]=t[n])}return a}var s=r.createContext({}),u=function(t){var e=r.useContext(s),n=e;return t&&(n="function"==typeof t?t(e):o(o({},e),t)),n},c=function(t){var e=u(t.components);return r.createElement(s.Provider,{value:e},t.children)},d={inlineCode:"code",wrapper:function(t){var e=t.children;return r.createElement(r.Fragment,{},e)}},p=r.forwardRef((function(t,e){var n=t.components,a=t.mdxType,i=t.originalType,s=t.parentName,c=l(t,["components","mdxType","originalType","parentName"]),p=u(n),f=a,g=p["".concat(s,".").concat(f)]||p[f]||d[f]||i;return n?r.createElement(g,o(o({ref:e},c),{},{components:n})):r.createElement(g,o({ref:e},c))}));function f(t,e){var n=arguments,a=e&&e.mdxType;if("string"==typeof t||a){var i=n.length,o=new Array(i);o[0]=p;var l={};for(var s in e)hasOwnProperty.call(e,s)&&(l[s]=e[s]);l.originalType=t,l.mdxType="string"==typeof t?t:a,o[1]=l;for(var u=2;u<i;u++)o[u]=n[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,n)}p.displayName="MDXCreateElement"},1012:function(t,e,n){n.r(e),n.d(e,{contentTitle:function(){return s},default:function(){return p},frontMatter:function(){return l},metadata:function(){return u},toc:function(){return c}});var r=n(7462),a=n(3366),i=(n(7294),n(3905)),o=["components"],l={title:"Industrializing our data pipeline"},s=void 0,u={unversionedId:"getting-started/part-2/industrializing",id:"getting-started/part-2/industrializing",title:"Industrializing our data pipeline",description:"Now that we have successfully loaded and analyzed the data, we show the results to our friend Tom.",source:"@site/docs/getting-started/part-2/industrializing.md",sourceDirName:"getting-started/part-2",slug:"/getting-started/part-2/industrializing",permalink:"/docs/getting-started/part-2/industrializing",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-2/industrializing.md",tags:[],version:"current",frontMatter:{title:"Industrializing our data pipeline"},sidebar:"docs",previous:{title:"Compute Distances",permalink:"/docs/getting-started/part-1/compute-distances"},next:{title:"Delta Lake - a better data format",permalink:"/docs/getting-started/part-2/delta-lake-format"}},c=[{value:"Agenda",id:"agenda",children:[],level:2}],d={toc:c};function p(t){var e=t.components,l=(0,a.Z)(t,o);return(0,i.kt)("wrapper",(0,r.Z)({},d,l,{components:e,mdxType:"MDXLayout"}),(0,i.kt)("p",null,"Now that we have successfully loaded and analyzed the data, we show the results to our friend Tom.\nHe is very satisfied with the results and would like to bring this data pipeline to production. He is especially interested in keeping all historical data, in order to analyze data over time.\nFrom your side you tell Tom, that the pipeline works well, but that CSV-Files are not a very stable and reliable data format, and that you'll suggest him a better solution for this. "),(0,i.kt)("h2",{id:"agenda"},"Agenda"),(0,i.kt)("p",null,"In Part 2 we will cover the following two points:"),(0,i.kt)("ol",null,(0,i.kt)("li",{parentName:"ol"},"Using a better, transactional data format: Delta Lake"),(0,i.kt)("li",{parentName:"ol"},"Keeping historical data: Historization and Deduplication")),(0,i.kt)("p",null,"Additionally we will use Polynote-Notebook to easily interact with our data."),(0,i.kt)("p",null,"Part 2 is based on ",(0,i.kt)("a",{target:"_blank",href:n(1159).Z},"this")," configuration file, ",(0,i.kt)("strong",{parentName:"p"},"copy it to config/application.conf")," to walk through the tutorial."))}p.isMDXComponent=!0},1159:function(t,e,n){e.Z=n.p+"assets/files/application-part1-compute-final-fcc8c1494d45a8500eedd508e0245aff.conf"}}]);