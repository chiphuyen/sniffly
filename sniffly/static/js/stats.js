// Statistics functions for Claude Analytics Dashboard
// This module contains pure data processing and calculation functions
// extracted from dashboard.html for better organization and reusability

// Date and Time Calculations
function calculateDateRange(statistics) {
  // Handle null date range (common for empty rollups)
  if (!statistics.overview.date_range.start || !statistics.overview.date_range.end) {
    // Check if this is an empty rollup
    if (statistics.is_rollup && statistics.child_project_count === 0) {
      return 'No projects';
    }
    return '0 days';
  }
  
  const start = new Date(statistics.overview.date_range.start);
  const end = new Date(statistics.overview.date_range.end);
  
  // Check for invalid dates
  if (isNaN(start.getTime()) || isNaN(end.getTime())) {
    return '0 days';
  }
  
  const days = Math.floor((end - start) / (24 * 60 * 60 * 1000));
  return `${days} days`;
}

function calculateDaysInclusive(statistics) {
  // Handle null date range (common for empty rollups)
  if (!statistics.overview.date_range.start || !statistics.overview.date_range.end) {
    return 0;
  }
  
  const start = new Date(statistics.overview.date_range.start);
  const end = new Date(statistics.overview.date_range.end);
  
  // Check for invalid dates
  if (isNaN(start.getTime()) || isNaN(end.getTime())) {
    return 0;
  }
  
  // Add 1 to make it inclusive (e.g., Jun 26 to Jun 28 = 3 days)
  const days = Math.floor((end - start) / (24 * 60 * 60 * 1000)) + 1;
  return days;
}

// Tool Usage Statistics
function calculateTotalToolCalls(statistics) {
  // Sum up all tool usage counts
  let total = 0;
  if (statistics.tools && statistics.tools.usage_counts) {
    for (const count of Object.values(statistics.tools.usage_counts)) {
      total += count;
    }
  }
  return total;
}

function calculateDistinctTools(statistics) {
  // Count unique tools used
  if (statistics.tools && statistics.tools.usage_counts) {
    return Object.keys(statistics.tools.usage_counts).length;
  }
  return 0;
}

// Cache Statistics
function calculateCacheEfficiency(statistics) {
  const created = statistics.overview.total_tokens.cache_creation || 0;
  const read = statistics.overview.total_tokens.cache_read || 0;
  if (created === 0) {return 0;}
  return Math.min(100, Math.round((read / created) * 100));
}

function formatCostSaved(statistics) {
  if (!statistics.cache) {return '0 units';}
  const saved = statistics.cache.cost_saved_base_units;
  if (saved === 0) {return '0 units';}
  if (saved < 0) {
    return `−${formatNumber(Math.abs(saved))} units`;
  }
  return `${formatNumber(saved)}`;
}

// Token Analysis
function calculateHourlyTokens(allMessages) {
  const hourlyInput = new Array(24).fill(0);
  const hourlyOutput = new Array(24).fill(0);
    
  allMessages.forEach(msg => {
    if (msg.timestamp) {
      try {
        const hour = new Date(msg.timestamp).getHours();
        hourlyInput[hour] += msg.tokens.input || 0;
        hourlyOutput[hour] += msg.tokens.output || 0;
      } catch (e) {
        // Skip invalid timestamps
      }
    }
  });
    
  return {
    input: hourlyInput,
    output: hourlyOutput
  };
}

// Generate data for the entire project duration with adaptive bucketing (max 60 points)
function generateProjectDurationData(allMessages, maxDataPoints = 60) {
  // Get date range
  const timestamps = allMessages
    .filter(m => m.timestamp)
    .map(m => new Date(m.timestamp));
    
  if (timestamps.length === 0) {
    return { labels: [], counts: [], inputTokens: [], outputTokens: [], sparseLabels: [] };
  }
    
  const minDate = new Date(Math.min(...timestamps));
  const maxDate = new Date(Math.max(...timestamps));
  const totalDuration = maxDate - minDate;
    
  // Calculate optimal bucket size to stay under maxDataPoints
  const hourMs = 60 * 60 * 1000;
  const totalHours = Math.ceil(totalDuration / hourMs);
  const hoursPerBucket = Math.ceil(totalHours / maxDataPoints);
  const bucketSize = hoursPerBucket * hourMs;
    
  // Determine date format based on bucket size
  let dateFormat;
  const dayMs = 24 * hourMs;
  if (bucketSize < 4 * hourMs) {
    // Hourly or sub-4-hour buckets
    dateFormat = { month: 'short', day: 'numeric', hour: 'numeric', hour12: true };
  } else if (bucketSize < dayMs) {
    // 4-hour to daily buckets
    dateFormat = { month: 'short', day: 'numeric', hour: 'numeric', hour12: true };
  } else if (bucketSize < 7 * dayMs) {
    // Daily to weekly buckets
    dateFormat = { month: 'short', day: 'numeric' };
  } else {
    // Weekly or larger buckets
    dateFormat = { year: 'numeric', month: 'short', day: 'numeric' };
  }
    
  // Create buckets
  const buckets = [];
  let currentTime = new Date(minDate);
  currentTime.setMinutes(0, 0, 0);
    
  // Align to bucket boundaries
  if (bucketSize >= dayMs) {
    currentTime.setHours(0);
  }
    
  while (currentTime <= maxDate) {
    buckets.push({
      start: new Date(currentTime),
      end: new Date(currentTime.getTime() + bucketSize),
      count: 0,
      inputTokens: 0,
      outputTokens: 0
    });
    currentTime = new Date(currentTime.getTime() + bucketSize);
  }
    
  // Fill buckets with data
  allMessages.forEach(msg => {
    if (msg.timestamp) {
      const msgDate = new Date(msg.timestamp);
            
      // Find the appropriate bucket
      for (const bucket of buckets) {
        if (msgDate >= bucket.start && msgDate < bucket.end) {
          bucket.count++;
          bucket.inputTokens += msg.tokens.input || 0;
          bucket.outputTokens += msg.tokens.output || 0;
          break;
        }
      }
    }
  });
    
  // Create labels
  const labels = buckets.map(bucket => {
    // For multi-day buckets, show range
    if (bucketSize >= dayMs && bucket.end - bucket.start > dayMs) {
      const startStr = bucket.start.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      const endStr = new Date(bucket.end - 1).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
      return `${startStr} - ${endStr}`;
    }
    return bucket.start.toLocaleString('en-US', dateFormat);
  });
    
  // Create sparse labels for display
  const sparseLabels = [];
  const dataPointCount = buckets.length;
    
  if (dataPointCount <= 15) {
    // Show all labels if 15 or fewer data points
    sparseLabels.push(...labels);
  } else {
    // Smart labeling to show ~10-15 labels
    const labelInterval = Math.ceil(dataPointCount / 12);
        
    buckets.forEach((bucket, index) => {
      if (index === 0 || index === dataPointCount - 1) {
        // Always show first and last
        sparseLabels[index] = labels[index];
      } else if (index % labelInterval === 0) {
        // Show at intervals
        sparseLabels[index] = labels[index];
      } else {
        sparseLabels[index] = '';
      }
    });
  }
    
  return {
    labels: labels,
    counts: buckets.map(b => b.count),
    inputTokens: buckets.map(b => b.inputTokens),
    outputTokens: buckets.map(b => b.outputTokens),
    sparseLabels: sparseLabels,
    bucketSize: bucketSize,
    bucketCount: buckets.length
  };
}

// Data Generation
function generateHourlyData(allMessages) {
  // Get date range
  const timestamps = allMessages
    .filter(m => m.timestamp)
    .map(m => new Date(m.timestamp));
    
  if (timestamps.length === 0) {
    return { labels: [], counts: [], inputTokens: [], outputTokens: [] };
  }
    
  const minDate = new Date(Math.min(...timestamps));
  const maxDate = new Date(Math.max(...timestamps));
    
  // Create hourly buckets
  const hourlyBuckets = new Map();
  let currentHour = new Date(minDate);
  currentHour.setMinutes(0, 0, 0);
    
  while (currentHour <= maxDate) {
    const key = currentHour.toISOString();
    hourlyBuckets.set(key, {
      count: 0,
      inputTokens: 0,
      outputTokens: 0
    });
    currentHour = new Date(currentHour.getTime() + 60 * 60 * 1000); // Add 1 hour
  }
    
  // Fill buckets with data
  allMessages.forEach(msg => {
    if (msg.timestamp) {
      const msgDate = new Date(msg.timestamp);
      const hourKey = new Date(msgDate);
      hourKey.setMinutes(0, 0, 0);
      const key = hourKey.toISOString();
            
      if (hourlyBuckets.has(key)) {
        const bucket = hourlyBuckets.get(key);
        bucket.count++;
        bucket.inputTokens += msg.tokens.input || 0;
        bucket.outputTokens += msg.tokens.output || 0;
      }
    }
  });
    
  // Convert to arrays
  const sortedEntries = Array.from(hourlyBuckets.entries()).sort((a, b) => a[0].localeCompare(b[0]));
    
  // Limit to last 48 hours for readability
  const last48Hours = sortedEntries.slice(-48);
    
  return {
    labels: last48Hours.map(([timestamp]) => {
      const date = new Date(timestamp);
      return date.toLocaleString('en-US', { 
        month: 'short', 
        day: 'numeric', 
        hour: 'numeric',
        hour12: true
      });
    }),
    counts: last48Hours.map(([, data]) => data.count),
    inputTokens: last48Hours.map(([, data]) => data.inputTokens),
    outputTokens: last48Hours.map(([, data]) => data.outputTokens)
  };
}

function buildChronologicalData(allMessages, originalCommandDetails, currentJsonlData) {
  const chronologicalData = [];
    
  // Add all messages
  if (allMessages && allMessages.length > 0) {
    chronologicalData.push(...allMessages);
  }
    
  // Add all commands
  if (originalCommandDetails && originalCommandDetails.length > 0) {
    chronologicalData.push(...originalCommandDetails);
  }
    
  // Add all JSONL entries if loaded
  if (currentJsonlData && currentJsonlData.length > 0) {
    chronologicalData.push(...currentJsonlData);
  }
    
  // Sort by timestamp (oldest first)
  chronologicalData.sort((a, b) => {
    const aTime = a.timestamp || '';
    const bTime = b.timestamp || '';
    return aTime.localeCompare(bTime);
  });
    
  return chronologicalData;
}

// Calculate daily costs from raw messages in local timezone
function calculateDailyCosts(allMessages) {
  const dailyData = {};
    
  // Group messages by local date
  allMessages.forEach(msg => {
    if (msg.timestamp) {
      const localDate = new Date(msg.timestamp);
      const dateKey = localDate.toLocaleDateString('en-CA'); // YYYY-MM-DD format in local time
            
      if (!dailyData[dateKey]) {
        dailyData[dateKey] = {
          messages: 0,
          tokens: { input: 0, output: 0, cache_creation: 0, cache_read: 0 },
          models: {}
        };
      }
            
      dailyData[dateKey].messages++;
            
      // Accumulate tokens
      for (const [key, value] of Object.entries(msg.tokens || {})) {
        dailyData[dateKey].tokens[key] = (dailyData[dateKey].tokens[key] || 0) + value;
      }
            
      // Track tokens by model for cost calculation
      if (msg.type === 'assistant' && msg.model && msg.model !== 'N/A') {
        const model = msg.model;
        if (!dailyData[dateKey].models[model]) {
          dailyData[dateKey].models[model] = {
            tokens: { input: 0, output: 0, cache_creation: 0, cache_read: 0 },
            count: 0
          };
        }
        dailyData[dateKey].models[model].count++;
        for (const [key, value] of Object.entries(msg.tokens || {})) {
          dailyData[dateKey].models[model].tokens[key] = 
                        (dailyData[dateKey].models[model].tokens[key] || 0) + value;
        }
      }
    }
  });
    
  // Calculate costs for each day
  const result = {};
  for (const [date, data] of Object.entries(dailyData)) {
    let totalCost = 0;
    const modelCosts = {};
        
    // Use PricingUtils if available
    if (window.PricingUtils && window.PricingUtils.calculateCost) {
      for (const [model, modelData] of Object.entries(data.models)) {
        const costBreakdown = window.PricingUtils.calculateCost(modelData.tokens, model);
        modelCosts[model] = costBreakdown;
        totalCost += costBreakdown.total_cost;
      }
    }
        
    result[date] = {
      messages: data.messages,
      tokens: data.tokens,
      cost: {
        total: totalCost,
        by_model: modelCosts
      }
    };
  }
    
  return result;
}

// Calculate total project cost
function calculateTotalProjectCost(allMessages) {
  if (!allMessages || !window.PricingUtils) {
    return null;
  }
    
  // Handle case where messages might be wrapped in an object
  if (!Array.isArray(allMessages)) {
    console.warn('calculateTotalProjectCost: messages is not an array, attempting to extract');
    if (allMessages.messages && Array.isArray(allMessages.messages)) {
      allMessages = allMessages.messages;
    } else {
      console.error('calculateTotalProjectCost: Unable to extract messages array');
      return null;
    }
  }
    
  let totalCost = 0;
  const modelCosts = {};
    
  // Group tokens by model
  allMessages.forEach(msg => {
    if (msg.type === 'assistant' && msg.model && msg.model !== 'N/A') {
      if (!modelCosts[msg.model]) {
        modelCosts[msg.model] = {
          input: 0,
          output: 0,
          cache_creation: 0,
          cache_read: 0
        };
      }
            
      modelCosts[msg.model].input += msg.tokens.input || 0;
      modelCosts[msg.model].output += msg.tokens.output || 0;
      modelCosts[msg.model].cache_creation += msg.tokens.cache_creation || 0;
      modelCosts[msg.model].cache_read += msg.tokens.cache_read || 0;
    }
  });
    
  // Calculate costs for each model
  for (const [model, tokens] of Object.entries(modelCosts)) {
    const costs = window.PricingUtils.calculateCost(tokens, model);
    totalCost += costs.total_cost;
  }
    
  return totalCost;
}

// Generate daily token data for the last N days
function generateDailyTokenData(allMessages, maxDays = 30) {
  const dailyData = {};
    
  // Group messages by local date
  allMessages.forEach(msg => {
    if (msg.timestamp) {
      const localDate = new Date(msg.timestamp);
      const dateKey = localDate.toLocaleDateString('en-CA'); // YYYY-MM-DD format in local time
            
      if (!dailyData[dateKey]) {
        dailyData[dateKey] = {
          input: 0,
          output: 0,
          cache_creation: 0,
          cache_read: 0
        };
      }
            
      // Accumulate tokens
      dailyData[dateKey].input += msg.tokens.input || 0;
      dailyData[dateKey].output += msg.tokens.output || 0;
      dailyData[dateKey].cache_creation += msg.tokens.cache_creation || 0;
      dailyData[dateKey].cache_read += msg.tokens.cache_read || 0;
    }
  });
    
  // Sort dates and limit to last N days
  const sortedDates = Object.keys(dailyData).sort();
  const datesToShow = sortedDates.slice(-maxDays);
    
  // Create arrays for Chart.js
  const labels = [];
  const inputTokens = [];
  const outputTokens = [];
    
  datesToShow.forEach(date => {
    // Parse date as local midnight to avoid timezone shifts
    const [year, month, day] = date.split('-').map(Number);
    const localDate = new Date(year, month - 1, day); // month is 0-indexed
    labels.push(localDate.toLocaleDateString('en-US', { 
      month: 'short', 
      day: 'numeric' 
    }));
        
    inputTokens.push(dailyData[date].input);
    outputTokens.push(dailyData[date].output);
  });
    
  return {
    labels,
    inputTokens,
    outputTokens,
    dailyData: datesToShow.map(date => dailyData[date])
  };
}

// Export all functions as StatsModule
const StatsModule = {
  // Date and Time Calculations
  calculateDateRange,
  calculateDaysInclusive,
    
  // Tool Usage Statistics
  calculateTotalToolCalls,
  calculateDistinctTools,
    
  // Cache Statistics
  calculateCacheEfficiency,
  formatCostSaved,
    
  // Token Analysis
  calculateHourlyTokens,
    
  // Data Generation
  generateHourlyData,
  generateDailyTokenData,
  generateProjectDurationData,
  buildChronologicalData,
  calculateDailyCosts,
    
  // Cost Calculations
  calculateTotalProjectCost
};

// Make available globally for browser usage
if (typeof window !== 'undefined') {
  window.StatsModule = StatsModule;
}

// Export for module usage
if (typeof module !== 'undefined' && module.exports) {
  module.exports = StatsModule;
}