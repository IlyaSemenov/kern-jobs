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
    nav: [
      { text: "Docs", link: "/getting-started" },
      {
        text: "Kern",
        items: [
          { text: "Kern Libraries", link: "https://ilyasemenov.github.io/kern-orm/ecosystem" },
          { text: "ORM", link: "https://ilyasemenov.github.io/kern-orm/orm/getting-started" },
          {
            text: "Database",
            link: "https://ilyasemenov.github.io/kern-orm/database/getting-started",
          },
          {
            text: "Migrations",
            link: "https://ilyasemenov.github.io/kern-orm/migrations/getting-started",
          },
        ],
      },
    ],
    sidebar: [
      {
        text: "Introduction",
        items: [
          { text: "Getting Started", link: "/getting-started" },
          { text: "Comparison", link: "/comparison" },
          { text: "Database Clients and Transactions", link: "/database-clients" },
        ],
      },
      {
        text: "Tasks",
        items: [
          { text: "Defining Tasks", link: "/tasks/" },
          { text: "Deduplication and Debouncing", link: "/tasks/keys" },
          { text: "Sequential Queues", link: "/tasks/queues" },
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
          { text: "Scheduling Recurring Jobs", link: "/workers/schedules" },
          { text: "Leases", link: "/workers/leases" },
          { text: "Graceful Shutdown", link: "/workers/stopping" },
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
