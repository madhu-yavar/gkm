# GKM Frontend

React + TypeScript + Vite frontend for the GKM tax return dashboard application.

## Development

```bash
# Install dependencies
npm install

# Run development server (http://localhost:5173)
npm run dev

# Build for production
npm run build

# Lint code
npm run lint

# Preview production build
npm run preview
```

## Environment

Create `.env` file:
```
VITE_API_BASE=http://localhost:8000
```

## Architecture

- **Auth**: JWT-based with localStorage persistence
- **Routing**: React Router v7 with protected routes
- **State**: React Context for auth and dashboard data
- **API**: Centralized `useApi()` hook with TypeScript types
- **Styling**: Tailwind CSS with `tailwindcss-animate`

## Pages

- `/login` - User authentication
- `/dashboard` - KPIs, clients, staff tables
- `/documents` - Excel upload and processing
- `/settings` - Gemini API configuration

## Key Features

- Excel workbook preview with PII field selection
- Async document processing job tracking
- Dashboard blueprint proposal and approval
- PDF report downloads (overall and client-specific)
- Gemini AI integration for intelligent processing
