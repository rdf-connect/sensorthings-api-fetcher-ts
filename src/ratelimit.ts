// ratelimit.ts
type QueuedRequest = {
    url: string;
    delay: number;
    resolve: (value: Response) => void;
    reject: (reason?: string) => void;
};

const requestQueue: QueuedRequest[] = [];
let isProcessing = false;

/**
 * Rate limit all fetch requests on a 250 ms delay minimum
 */
export function rateLimitedFetch(
    url: string,
    delay: number = 250,
): Promise<Response> {
    return new Promise((resolve, reject) => {
        requestQueue.push({ url, delay, resolve, reject });
        processQueue();
    });
}

async function processQueue() {
    if (isProcessing || requestQueue.length === 0) {
        return;
    }

    isProcessing = true;

    const { url, delay, resolve, reject } = requestQueue.shift()!;

    try {
        const response = await fetch(url);
        resolve(response);
    } catch (err) {
        reject(err);
    }

    // Use the request’s own delay before moving on
    setTimeout(() => {
        isProcessing = false;
        processQueue();
    }, delay);
}
