# Work Order Scheduling Conflicts Report Implementation

## Overview
A comprehensive Work Order Scheduling Conflicts report has been successfully implemented across the entire EvoTech stack. This report identifies and analyzes potential scheduling conflicts between consecutive work orders based on geographic distance and timing constraints.

## Backend Implementation (EvoAPI)

### 1. Data Transfer Objects (DTOs)
**File**: `src/EvoAPI.Shared/DTOs/WorkOrderSchedulingConflictsDto.cs`
- `WorkOrderSchedulingConflictsDto`: Main conflict details including technician, risk level, travel time, and work order information
- `WorkOrderSchedulingConflictsSummaryDto`: Summary statistics by conflict risk type
- `WorkOrderSchedulingConflictsReportDto`: Container for both conflicts and summary data

### 2. Data Service Interface
**File**: `src/EvoAPI.Core/Interfaces/IDataService.cs`
- Added `GetWorkOrderSchedulingConflictsAsync()` method
- Added `GetWorkOrderSchedulingConflictsSummaryAsync()` method

### 3. Data Service Implementation
**File**: `src/EvoAPI.Core/Services/DataService.cs`
- Implemented complex SQL query to analyze consecutive work orders
- Calculates travel time between work order locations
- Assesses geographic proximity (same zip, city, state, different state)
- Identifies risk levels: OVERLAPPING, IMPOSSIBLE, HIGH_RISK, MEDIUM_RISK, REVIEW_NEEDED
- Includes proper audit logging and error handling

### 4. Reports Controller
**File**: `src/EvoAPI.Api/Controllers/ReportsController.cs`
- Added `GetWorkOrderSchedulingConflicts()` endpoint at `/EvoApi/reports/work-order-scheduling-conflicts`
- Admin-only access with `[AdminOnly]` attribute
- Returns both detailed conflicts and summary statistics
- Includes proper error handling and audit logging

## Frontend Implementation (EvoTech)

### 1. API Integration
**File**: `src/components/api.js`
- Added `GetWorkOrderSchedulingConflicts()` method using evoApi instance
- Properly exposed method on main api export for component access
- Includes error handling and response validation

### 2. Report Page Component
**File**: `src/pages/office/reports/work-order-scheduling-conflicts.js`
- Comprehensive React component with Chart.js integration
- Displays summary metrics cards showing total conflicts, critical issues, etc.
- Interactive pie chart showing risk distribution
- Bar chart showing technicians with most conflicts
- Detailed table with all conflict information
- Proper loading states and error handling
- Mobile-responsive design

### 3. Styling
**File**: `src/pages/office/reports/work-order-scheduling-conflicts.module.css`
- Professional styling matching existing report pages
- Color-coded risk badges (red for overlapping, orange for high risk, etc.)
- Responsive grid layouts for metrics and charts
- Detailed table styling with hover effects
- Mobile-optimized responsive breakpoints

### 4. Navigation Integration
**File**: `src/pages/office/reports/index.js`
- Added new report to Operations Management section
- Updated item count from 3 to 4
- Used warning icon (⚠️) to represent scheduling conflicts
- Set as available report (not "coming soon")

## Key Features

### Risk Assessment Categories
1. **OVERLAPPING**: Work orders with overlapping time periods (critical)
2. **IMPOSSIBLE**: Zero travel time between different locations (critical) 
3. **HIGH_RISK**: ≤15 minutes travel time between different states
4. **MEDIUM_RISK**: ≤30 minutes travel time within same state
5. **REVIEW_NEEDED**: ≤60 minutes travel time between different locations

### Dashboard Metrics
- Total Conflicts: Overall count of scheduling issues
- Critical Issues: Sum of overlapping, impossible, and high-risk conflicts
- Critical Rate: Percentage of conflicts that are critical
- Individual counters for each risk category

### Visual Analytics
- **Pie Chart**: Distribution of conflicts by risk level with color coding
- **Bar Chart**: Top 8 technicians with most scheduling conflicts
- **Detailed Table**: Complete conflict information with:
  - Risk badges with appropriate colors
  - Technician names and organization changes
  - Travel time formatting (minutes/hours)
  - Work order details and timing
  - Geographic location information

### Data Analysis
- Analyzes future work orders (from current date forward)
- Excludes Metro Pipe Program Administration
- Requires work orders to have end times
- Considers only consecutive work orders within 2-hour windows
- Provides organization change indicators (company/call center switches)

## Database Query Logic
The implementation uses sophisticated SQL with Common Table Expressions (CTEs):

1. **WorkOrderData CTE**: Gathers all future work orders with location data
2. **ConsecutiveWorkOrders CTE**: Identifies consecutive work orders per technician
3. **PotentialConflicts CTE**: Calculates risk levels based on time and location
4. **Final Query**: Returns filtered results ordered by risk priority

## Security & Performance
- Admin-only access protection using `[AdminOnly]` attribute
- Proper authentication via BaseController inheritance
- Audit logging for all operations
- Error handling with structured responses
- Optimized SQL queries with proper indexing considerations
- Frontend prevents duplicate API calls with ref-based caching

## Integration Notes
- Follows established EvoAPI patterns for consistency
- Uses existing authentication and authorization systems
- Integrates seamlessly with existing reports dashboard
- Maintains styling consistency with other report pages
- Includes proper Chart.js integration following Next.js patterns

## Usage
1. Navigate to Office → Reports → Work Order Scheduling Conflicts
2. View summary metrics for quick overview
3. Analyze visual charts for patterns and trends
4. Review detailed table for specific conflict resolution
5. Export or print report data as needed

This implementation provides operations teams with critical insights into scheduling efficiency and helps prevent service disruptions caused by impossible or impractical work order sequences.
