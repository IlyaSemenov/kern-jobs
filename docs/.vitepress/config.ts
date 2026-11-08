import { defineConfig } from "vitepress"

export default defineConfig({
  title: "kern-jobs",
  description:
    "Background jobs on PostgreSQL for TypeScript: typed tasks, transactional enqueue, keys, debounce, queues, and leases.",
  // Served from https://ilyasemenov.github.io/kern-jobs/
  base: "/kern-jobs/",
  cleanUrls: true,
  lastUpdated: true,
  themeConfig: {
    nav: [{ text: "Docs", link: "/getting-started" }],
    sidebar: [
      {
        text: "Introduction",
        items: [
          { text: "Getting Started", link: "/getting-started" },
          { text: "Database Clients", link: "/database-clients" },
        ],
      },
      {
        text: "Tasks",
        items: [
          { text: "Defining Tasks", link: "/tasks/" },
          { text: "Keys", link: "/tasks/keys" },
          { text: "Queues", link: "/tasks/queues" },
          { text: "Scopes", link: "/tasks/scopes" },
        ],
      },
      {
        text: "Workers",
        items: [
          { text: "Running Workers", link: "/workers/" },
          { text: "Writing Handlers", link: "/workers/handlers" },
          { text: "Retries", link: "/workers/retries" },
          { text: "Failed Jobs", link: "/workers/failed-jobs" },
          { text: "Leases", link: "/workers/leases" },
          { text: "Stopping", link: "/workers/stopping" },
          { text: "Monitoring", link: "/workers/monitoring" },
        ],
      },
      {
        text: "Deployment",
        items: [
          { text: "Updating kern-jobs", link: "/deployment/updating-kern-jobs" },
          { text: "Changing Tasks", link: "/deployment/changing-tasks" },
        ],
      },
    ],
    search: { provider: "local" },
    editLink: {
      pattern: "https://github.com/IlyaSemenov/kern-jobs/edit/main/docs/:path",
    },
    socialLinks: [{ icon: "github", link: "https://github.com/IlyaSemenov/kern-jobs" }],
  },
})
