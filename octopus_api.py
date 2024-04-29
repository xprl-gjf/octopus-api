import asyncio
import time
from typing import List, Dict, Any, Optional

import aiohttp
from aiolimiter import AsyncLimiter
from tqdm import tqdm


class TentacleSession(aiohttp.ClientSession):
    """ TentacleSession is a wrapper around the aiohttp.ClientSession, where it introduces retry functionality.

        Args:
            retries (int): The number of retries for an unsuccessful request.
            retry_sleep (float): The time service sleeps between nonsuccessful request calls. Defaults to 1.0.
            kwargs: Additional args passed through to the underlying aiohttp.ClientSession.

        Returns:
            TentacleSession(aiohttp.ClientSession)
    """

    def __init__(self, retries=3, retry_sleep=1.0, **kwargs):
        self.retries = retries
        self.retry_sleep = retry_sleep
        super().__init__(raise_for_status=True, **kwargs)

    def __retry__(self, func, **kwargs) -> Any:
        attempts = 0
        error = Exception()
        while attempts < self.retries:
            try:
                return func(**kwargs)
            except Exception as error:
                attempts += 1
                error = error
                time.sleep(self.retry_sleep)

        raise error

    def get(self, **kwargs) -> Any:
        return self.__retry__(func=super().get, **kwargs)

    def patch(self, **kwargs) -> Any:
        return self.__retry__(func=super().patch, **kwargs)

    def post(self, **kwargs) -> Any:
        return self.__retry__(func=super().post, **kwargs)

    def put(self, **kwargs) -> Any:
        return self.__retry__(func=super().put, **kwargs)

    def request(self, **kwargs) -> Any:
        return self.__retry__(func=super().request, **kwargs)


class OctopusApi:
    """ Initiates the Octopus client.
        Args:
            limiter (Optional[AsyncLimiter]): The session task rate limiter.
            connections (Optional[int]): Maximum connections on the given endpoint, defaults to 5.
            retries (int): The number of retries for an unsuccessful request, defaults to 3.

        Returns:
            OctopusApi
    """

    class __NullAsyncLimiter:
        async def __aenter__(self): pass
        async def __aexit__(self, exc_type, exc, tb): pass

    __null_limiter = __NullAsyncLimiter()

    def __init__(self, limiter: Optional[AsyncLimiter] = None, connections: int = 5, retries: int = 3):
        self.limiter: Optional[AsyncLimiter] = limiter
        self.connections: int = connections
        self.retries: int = retries

    def get_coroutine(self, requests_list: List[Dict[str, Any]], func: callable):

        async def __tentacles__(limiter: Optional[AsyncLimiter], retries: int, connections: int,
                                requests_list: List[Dict[str, Any]], func: callable) -> List[Any]:

            responses_order: Dict = {}
            progress_bar = tqdm(total=len(requests_list))
            retry_sleep = limiter.time_period / limiter.max_rate if limiter else 0

            async def func_mod(session: TentacleSession, limiter: Optional[AsyncLimiter], request: Dict, itr: int):
                async with limiter if limiter else OctopusApi.__null_limiter:
                    resp = await func(session, request)
                    responses_order[itr] = resp
                    progress_bar.update()

            conn = aiohttp.TCPConnector(limit_per_host=connections)
            async with TentacleSession(retries=retries,
                                       retry_sleep=retry_sleep * self.connections * 2.0 if retry_sleep else 1,
                                       connector=conn) as session:

                tasks = set()
                for itr, request in enumerate(requests_list):
                    if len(tasks) >= self.connections:
                        # handle connections overflow by waiting for at least one running task to complete...
                        _done, tasks = await asyncio.wait(
                            tasks, return_when=asyncio.FIRST_COMPLETED)
                    tasks.add(asyncio.create_task(func_mod(session, limiter, request, itr)))
                await asyncio.wait(tasks)
                return [value for (key, value) in sorted(responses_order.items())]

        return __tentacles__(self.limiter, self.retries, self.connections, requests_list, func)

    def execute(self, requests_list: List[Dict[str, Any]], func: callable) -> List[Any]:
        """ Execute the requests given the functions instruction.

            Empower asyncio libraries for performing parallel executions of the user-defined function.
            Given a list of requests, the result is ordered list of what the user-defined function returns.

            Args:
                requests_list (List[Dict[str, Any]): The list of requests in a dictionary format,
                  e.g. `[{"url": "http://example.com", "params": {...}, "body": {...}}..]`
                func (callable): The user-defined function to execute. This function takes in the following arguments:

                  - session (TentacleSession): The Octopus wrapper around the aiohttp.ClientSession.
                  - request (Dict): The request within the requests_list above.

            Returns:
                List(func->return)
        """

        result = asyncio.run(self.get_coroutine(requests_list, func))
        if result:
            return result
        return []
