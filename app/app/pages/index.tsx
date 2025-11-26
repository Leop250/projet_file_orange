import { useLoadScript } from '@react-google-maps/api';
import type { NextPage } from 'next';
import { useMemo } from 'react';

const Home: NextPage = () => {
  const libraries = useMemo(() => ['places'], []);

  const { isLoaded } = useLoadScript({
    googleMapsApiKey: process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY as string,
    libraries: libraries as any,
  });

  if (!isLoaded) {
    return <p>Loading...</p>;
  }

  return <div className="flex min-h-screen items-center justify-center bg-zinc-50 font-sans dark:bg-black">Map Script Loaded...</div>;
};

export default Home;