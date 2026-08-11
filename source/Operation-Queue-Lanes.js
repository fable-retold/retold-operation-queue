/**
 * Lanes: three preemptive priority bands for operation groups.
 *
 * A group's lane is inherited by its tasks; raising a group's lane makes its work preempt lower-lane
 * work. Lanes map to a numeric priority (higher = sooner) that an execution queue understands, so the
 * intent layer can hand a lane down as a concrete priority when it meters work out.
 */

const LANES = { URGENT: 'Urgent', NORMAL: 'Normal', BACKGROUND: 'Background' };

// Order low-to-high, and the numeric priority each lane maps to (higher = scheduled sooner).
const _ORDER = [ LANES.BACKGROUND, LANES.NORMAL, LANES.URGENT ];
const _PRIORITY = { Background: 100, Normal: 500, Urgent: 900 };

// Coerce any input to a known lane, defaulting to Normal. Case-insensitive.
function normalizeLane(pLane)
{
	let tmpLane = String((pLane === null || typeof pLane === 'undefined') ? '' : pLane).trim().toLowerCase();
	let tmpMatch = _ORDER.find((pKnown) => pKnown.toLowerCase() === tmpLane);
	return tmpMatch || LANES.NORMAL;
}

// The numeric priority for a lane (higher = sooner).
function laneToPriority(pLane) { return _PRIORITY[normalizeLane(pLane)]; }

// Whether the input names a known lane (before normalization's default kicks in).
function isLane(pLane)
{
	let tmpLane = String((pLane === null || typeof pLane === 'undefined') ? '' : pLane).trim().toLowerCase();
	return _ORDER.some((pKnown) => pKnown.toLowerCase() === tmpLane);
}

module.exports = { LANES, ORDER: _ORDER, PRIORITY: _PRIORITY, normalizeLane, laneToPriority, isLane };
