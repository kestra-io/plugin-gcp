import { createApp } from "vue";
import "@kestra-io/artifact-sdk/style.css";
import { initApp } from "@kestra-io/artifact-sdk";
import App from "./App.vue";

const appInstance = createApp(App);
initApp(appInstance);
appInstance.mount("#app");
