// Stats cards module - shared between dashboard.html and share-viewer.js

window.StatsCardsModule = {
  displayOverviewStats: function(statistics) {
    const overview = statistics.overview;
    const messageTypes = overview.message_types;
        
    const statsHTML = `
            <div class="stat-card">
                <h3>User Commands</h3>
                <div class="value">${statistics.user_interactions ? statistics.user_interactions.user_commands_analyzed : 0}</div>
                <div class="breakdown">
                    <span>${statistics.user_interactions ? statistics.user_interactions.avg_tokens_per_command.toFixed(1) : '0.0'} tokens/cmd</span>
                    <span>${(() => {
                        if (!statistics.user_interactions) return '0 books';
                        const totalWords = statistics.user_interactions.user_commands_analyzed * 
                                         statistics.user_interactions.avg_tokens_per_command * 
                                         3 / 4;  // tokens to words (3/4 ratio)
                        const books = totalWords / 60000;  // 60k words per book
                        return books >= 1 ? `${books.toFixed(1)} books` : `${books.toFixed(2)} books`;
                    })()}
                        <span class="tooltip-info-icon"
                              onmouseover="showTooltip('books-tooltip')" 
                              onmouseout="hideTooltip('books-tooltip')">ⓘ
                            <div id="books-tooltip" class="tooltip-dark position-below tooltip-sm">
                                The number of books you could've written.
                                Assuming 60k words/book.
                            </div>
                        </span>
                    </span>
                    <span>${StatsModule.calculateDaysInclusive(statistics)} days</span>
                    ${(() => {
                        // Show context info if there are compact summaries
                        const compactSummaryCount = messageTypes.compact_summary || 0;
                        if (compactSummaryCount > 0 && statistics.user_interactions) {
                            const commandsPerContext = Math.floor(statistics.user_interactions.user_commands_analyzed / compactSummaryCount);
                            return `<span>${commandsPerContext} cmds/full context</span>`;
                        }
                        return '';
                    })()}
                </div>
            </div>

            <div class="stat-card">
                <h3>User Interruption Rate
                    <span class="tooltip-info-icon"
                          onmouseover="showTooltip('interruption-tooltip')" 
                          onmouseout="hideTooltip('interruption-tooltip')">ⓘ</span>
                    <div id="interruption-tooltip" class="tooltip-dark position-below tooltip-sm">
                        <div style="font-size: 0.75rem; opacity: 0.9;">
                            % of instructions led to tool operations that needed manual intervention.
                        </div>
                    </div>
                </h3>
                <div class="value">${statistics.user_interactions.interruption_rate || 0}%</div>
                <div class="subtext">${statistics.user_interactions.commands_followed_by_interruption || 0} of ${statistics.user_interactions.non_interruption_commands || 0} commands</div>
            </div>
            
            ${statistics.user_interactions ? `
            <div class="stat-card">
                <h3>Steps per command</h3>
                <div class="value">${statistics.user_interactions.avg_steps_per_command || 0}</div>
                <div class="breakdown">
                    <span>${statistics.user_interactions.avg_tools_per_command || 0} tools/cmd</span>
                    <span>Longest chain: ${(() => {
                        const steps = (statistics.user_interactions.command_details || []).map(cmd => cmd.assistant_steps || 0);
                        return steps.length > 0 ? Math.max(...steps) : 0;
                    })()} steps</span>
                </div>
            </div>
            
            <div class="stat-card">
                <h3>Tool Use Rate
                    <span class="tooltip-info-icon"
                          onmouseover="showTooltip('tools-required-tooltip')" 
                          onmouseout="hideTooltip('tools-required-tooltip')">ⓘ</span>
                    <div id="tools-required-tooltip" class="tooltip-dark position-below tooltip-sm">
                        <div style="margin-bottom: 0.5rem;">Only actual user commands (not interruptions) are counted.</div>
                        <div style="margin-bottom: 0.25rem;">This is the number tools AI actually uses (not the tools it intends to use), before task completion or user interruption.</div>
                    </div>
                </h3>
                <div class="value">${statistics.user_interactions.percentage_requiring_tools}%</div>
                <div class="subtext">${statistics.user_interactions.commands_requiring_tools} of ${statistics.user_interactions.non_interruption_commands} commands</div>
                <div class="breakdown">
                    <span>${StatsModule.calculateDistinctTools(statistics)} tools</span>
                    <span>${StatsModule.calculateTotalToolCalls(statistics).toLocaleString()} tool calls</span>
                    ${(statistics.user_interactions && statistics.user_interactions.search_tool_percentage !== undefined) ? 
                        `<span>${statistics.user_interactions.search_tool_percentage}% search</span>` : 
                        ''
                    }
                </div>
            </div>

            <div class="stat-card">
                <h3>Project Cost
                    <span class="tooltip-info-icon"
                          onmouseover="showTooltip('total-cost-tooltip')" 
                          onmouseout="hideTooltip('total-cost-tooltip')">ⓘ</span>
                    <div id="total-cost-tooltip" class="tooltip-dark position-below tooltip-sm">
                        <div style="font-size: 0.85rem; line-height: 1.4;">
                            What you would've paid if you were using the Claude API directly.
                        </div>
                        <div style="font-size: 0.8rem; opacity: 0.8; margin-top: 0.5rem;">
                            Based on current token prices from LiteLLM
                        </div>
                    </div>
                </h3>
                <div class="value">
                    ${(() => {
    // Use pre-calculated total cost from statistics
    const totalCost = statistics.overview.total_cost;
    if (totalCost !== null && totalCost !== undefined) {
      return window.PricingUtils ? window.PricingUtils.formatCost(totalCost) : `$${totalCost.toFixed(2)}`;
    }
    return '$0.00';
  })()}
                </div>
                <div class="breakdown">
                    <span>Total tokens: ${formatNumber(overview.total_tokens.input + overview.total_tokens.output)}</span>
                    <span>Input: ${formatNumber(overview.total_tokens.input)}</span>
                    <span>Output: ${formatNumber(overview.total_tokens.output)}</span>
                </div>
            </div>

            <div class="stat-card">
                <h3>Prompt cache read
                    <span class="tooltip-info-icon"
                          onmouseover="showTooltip('cache-stats-tooltip')" 
                          onmouseout="hideTooltip('cache-stats-tooltip')">ⓘ</span>
                    <div id="cache-stats-tooltip" class="tooltip-dark position-below tooltip-sm">
                        <div style="margin-bottom: 0.5rem;"><strong>Hit Rate:</strong> % of assistant messages using cached content</div>
                        <div><strong>Cost Saved:</strong> Calculated as (Read × 0.9) - (Created × 0.25) in base token units. Cache creation costs 25% more, but reading costs 90% less.</div>
                    </div>
                </h3>
                <div class="value">${formatNumber(overview.total_tokens.cache_read)}</div>
                <div class="breakdown">
                    <span>${overview.total_messages.toLocaleString()} total messages</span>
                    <span>Created: ${formatNumber(overview.total_tokens.cache_creation)}</span>
                    <span>Cache hit rate: ${statistics.cache ? statistics.cache.hit_rate : 0}%</span>
                </div>
            </div>
            ` : ''}
        `;
        
    document.getElementById('overview-stats').innerHTML = statsHTML;
  }
};

// Helper function - copied from main dashboard
function formatNumber(num) {
  if (!num) {return '0';}
  if (num >= 1000000) {return (num / 1000000).toFixed(1) + 'M';}
  if (num >= 1000) {return (num / 1000).toFixed(1) + 'K';}
  return num.toLocaleString();
}