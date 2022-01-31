"use strict";(self.webpackChunksmart_data_lake=self.webpackChunksmart_data_lake||[]).push([[428],{3905:function(e,t,r){r.d(t,{Zo:function(){return d},kt:function(){return f}});var n=r(7294);function a(e,t,r){return t in e?Object.defineProperty(e,t,{value:r,enumerable:!0,configurable:!0,writable:!0}):e[t]=r,e}function o(e,t){var r=Object.keys(e);if(Object.getOwnPropertySymbols){var n=Object.getOwnPropertySymbols(e);t&&(n=n.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),r.push.apply(r,n)}return r}function i(e){for(var t=1;t<arguments.length;t++){var r=null!=arguments[t]?arguments[t]:{};t%2?o(Object(r),!0).forEach((function(t){a(e,t,r[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(r)):o(Object(r)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(r,t))}))}return e}function s(e,t){if(null==e)return{};var r,n,a=function(e,t){if(null==e)return{};var r,n,a={},o=Object.keys(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||(a[r]=e[r]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(n=0;n<o.length;n++)r=o[n],t.indexOf(r)>=0||Object.prototype.propertyIsEnumerable.call(e,r)&&(a[r]=e[r])}return a}var p=n.createContext({}),l=function(e){var t=n.useContext(p),r=t;return e&&(r="function"==typeof e?e(t):i(i({},t),e)),r},d=function(e){var t=l(e.components);return n.createElement(p.Provider,{value:t},e.children)},u={inlineCode:"code",wrapper:function(e){var t=e.children;return n.createElement(n.Fragment,{},t)}},c=n.forwardRef((function(e,t){var r=e.components,a=e.mdxType,o=e.originalType,p=e.parentName,d=s(e,["components","mdxType","originalType","parentName"]),c=l(r),f=a,m=c["".concat(p,".").concat(f)]||c[f]||u[f]||o;return r?n.createElement(m,i(i({ref:t},d),{},{components:r})):n.createElement(m,i({ref:t},d))}));function f(e,t){var r=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=r.length,i=new Array(o);i[0]=c;var s={};for(var p in t)hasOwnProperty.call(t,p)&&(s[p]=t[p]);s.originalType=e,s.mdxType="string"==typeof e?e:a,i[1]=s;for(var l=2;l<o;l++)i[l]=r[l];return n.createElement.apply(null,i)}return n.createElement.apply(null,r)}c.displayName="MDXCreateElement"},1745:function(e,t,r){r.r(t),r.d(t,{frontMatter:function(){return s},contentTitle:function(){return p},metadata:function(){return l},toc:function(){return d},default:function(){return c}});var n=r(7462),a=r(3366),o=(r(7294),r(3905)),i=["components"],s={title:"Get Departure Coordinates"},p=void 0,l={unversionedId:"getting-started/part-1/joining-departures-and-arrivals",id:"getting-started/part-1/joining-departures-and-arrivals",title:"Get Departure Coordinates",description:"Goal",source:"@site/docs/getting-started/part-1/joining-departures-and-arrivals.md",sourceDirName:"getting-started/part-1",slug:"/getting-started/part-1/joining-departures-and-arrivals",permalink:"/docs/getting-started/part-1/joining-departures-and-arrivals",editUrl:"https://github.com/smart-data-lake/smart-data-lake/tree/documentation/docs/getting-started/part-1/joining-departures-and-arrivals.md",tags:[],version:"current",frontMatter:{title:"Get Departure Coordinates"},sidebar:"docs",previous:{title:"Joining It Together",permalink:"/docs/getting-started/part-1/joining-it-together"},next:{title:"Compute Distances",permalink:"/docs/getting-started/part-1/compute-distances"}},d=[{value:"Goal",id:"goal",children:[],level:2},{value:"Define join_departures_airports action",id:"define-join_departures_airports-action",children:[],level:2},{value:"Define output object",id:"define-output-object",children:[],level:2},{value:"Try it out",id:"try-it-out",children:[],level:2}],u={toc:d};function c(e){var t=e.components,s=(0,a.Z)(e,i);return(0,o.kt)("wrapper",(0,n.Z)({},u,s,{components:t,mdxType:"MDXLayout"}),(0,o.kt)("h2",{id:"goal"},"Goal"),(0,o.kt)("p",null,"In this step we will extend the ",(0,o.kt)("a",{target:"_blank",href:r(1725).Z},"configuration file")," of the previous step\nso that we get the coordinates and the readable name of Bern Airport in our final data.\nSince we are dealing with just one record, we could manually add it to the data set.\nBut what if we wanted to extend our project to other departure airports in the future?\nWe'll do it in a generic way by adding another transformer into the action ",(0,o.kt)("em",{parentName:"p"},"join_departures_airports")),(0,o.kt)("h2",{id:"define-join_departures_airports-action"},"Define join_departures_airports action"),(0,o.kt)("p",null,"Let's start in an unusual way by first changing the action. You'll see why shortly."),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  join-departures-airports {\n    type = CustomSparkAction\n    inputIds = [stg-departures, int-airports]\n    outputIds = [btl-departures-arrivals-airports]\n    transformers = [{\n      type = SQLDfsTransformer\n      code = {\n        btl-connected-airports = """\n          select stg_departures.estdepartureairport, stg_departures.estarrivalairport, \n            airports.*\n          from stg_departures join int_airports airports on stg_departures.estArrivalAirport = airports.ident\n        """\n      }},\n    {\n      type = SQLDfsTransformer\n      code = {\n        btl-departures-arrivals-airports = """\n          select btl_connected_airports.estdepartureairport, btl_connected_airports.estarrivalairport,\n            btl_connected_airports.name as arr_name, btl_connected_airports.latitude_deg as arr_latitude_deg, btl_connected_airports.longitude_deg as arr_longitude_deg,\n            airports.name as dep_name, airports.latitude_deg as dep_latitude_deg, airports.longitude_deg as dep_longitude_deg\n          from btl_connected_airports join int_airports airports on btl_connected_airports.estdepartureairport = airports.ident\n        """\n      }\n    }    \n    ]\n    metadata {\n      feed = compute\n    }\n  }\n')),(0,o.kt)("p",null,"We added a second transformer of the type SQLDfsTransformer.\nIt's SQL Code references the result of the first transformer: ",(0,o.kt)("em",{parentName:"p"},"btl-connected-airports")," (remember the underscores, so ",(0,o.kt)("em",{parentName:"p"},"btl_connected_airports")," in SparkSQL).\nSDL will execute these transformations in the order you defined them, which allows you to chain them together, like we have done."),(0,o.kt)("p",null,"In the second SQL-Code, we join the result of the first SQL again with int_airports, but this time using ",(0,o.kt)("em",{parentName:"p"},"estdepartureairport")," as key\nto get the name and coordinates of the departures airport, Bern Airport.\nWe also renamed these columns so that they are distinguishable from the names and coordinates of the arrival airports.\nFinally, we put the result into a DataObject called ",(0,o.kt)("em",{parentName:"p"},"btl-departures-arrivals-airports"),"."),(0,o.kt)("h2",{id:"define-output-object"},"Define output object"),(0,o.kt)("p",null,"Let's add the new DataObject, as usual:"),(0,o.kt)("pre",null,(0,o.kt)("code",{parentName:"pre"},'  btl-departures-arrivals-airports {\n    type = CsvFileDataObject\n    path = "~{id}"\n  }\n')),(0,o.kt)("p",null,"Now we can simply delete the DataObject btl-connected-airports, because it is now only a temporary result within an action.\nThis is a key difference between chaining actions and chaining transformations within the same action:\nyou don't have intermediary results.\nAnother difference is that you cannot run an individual transformation alone, you can only run entire actions."),(0,o.kt)("h2",{id:"try-it-out"},"Try it out"),(0,o.kt)("p",null,(0,o.kt)("a",{target:"_blank",href:r(8918).Z},"This")," is how your config should look like by now."),(0,o.kt)("p",null,"When running the example, you should see a CSV file with departure and arrival airport names and coordinates."),(0,o.kt)("p",null,"Great! Now we have all the data we need in one place. The only thing left to do is to compute the distance\nbetween departure and arrival coordinates. Let's do that in the final step of part 1."))}c.isMDXComponent=!0},8918:function(e,t,r){t.Z=r.p+"assets/files/application-part1-compute-dep-arr-2c6918c75c2cb09fe8cf1210050a436b.conf"},1725:function(e,t,r){t.Z=r.p+"assets/files/application-part1-compute-join-dbcfb72a01b00f76fb01cf1a21c9b3ac.conf"}}]);